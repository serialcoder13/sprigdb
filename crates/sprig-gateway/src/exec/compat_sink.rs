//! MySQL's key names in 1062 on the MySQL persona.
//!
//! MariaDB names the key alone (`'PRIMARY'`); MySQL 8.4 names it with its
//! table (`'p.PRIMARY'`). Neither MariaDB's message nor the router's own
//! 1062 (from its lookup tables) says the table, so the statement
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

/// Resultsets as MySQL 8.4 sends them on the MySQL persona:
///
/// - computed integers are `LONGLONG` and `YEAR(..)` is `YEAR`, where
///   MariaDB sends `LONG` ([`sprig_sql::compat::intmeta`]):
///   the statement is parsed only when a resultset has an expression
///   column of a small integer type, and binary rows are re-encoded for
///   the changed columns;
/// - JSON values (type 245) are printed MySQL's way
///   ([`sprig_sql::compat::jsontext`]), in text and binary rows.
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
    /// `STATISTICS.EXPRESSION`: printed MySQL's way.
    exprs: Vec<usize>,
    /// Stored programs' text as written: every
    /// value with a marked segment (`sprig_sql::compat::stored::
    /// restore_original`), for statements that can read stored code.
    originals: bool,
    /// Columns of `exprs` holding a check's clause.
    checks: Vec<usize>,
    /// `SHOW` columns holding collation names (`Collation`,
    /// `collation_connection`, `Database Collation`, `SHOW CHARACTER
    /// SET`'s `Default collation`) and `SHOW CREATE
    /// DATABASE`'s text: MySQL's names.
    colls: Vec<usize>,
    /// `SHOW CREATE TRIGGER`'s statement: the head as
    /// MySQL rebuilds it ([`sprig_sql::compat::trigger`]).
    trigs: Vec<usize>,
    /// `SHOW PROCEDURE STATUS` and `SHOW FUNCTION STATUS`: MySQL's
    /// `Language` column (always `SQL`) goes in at this
    /// position of the client's columns (text rows; `after` stays the
    /// backend's shape).
    language: Option<usize>,
    /// A traditional `EXPLAIN`: `partitions` goes in at
    /// [`sprig_sql::compat::explain::PARTITIONS_AT`] (text rows).
    explain: bool,
    /// `EXPLAIN FORMAT=TREE` or `EXPLAIN ANALYZE`: MariaDB's
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
    #[allow(clippy::too_many_lines)] // one branch per kind of rewrite
    fn start(&mut self, columns: &[ColumnDef], format: RowFormat) -> Option<Vec<ColumnDef>> {
        self.cur = None;
        // MySQL's twelve EXPLAIN columns.
        // Binary rows too (a prepared EXPLAIN).
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
                checks: Vec::new(),
                colls: Vec::new(),
                trigs: Vec::new(),
                language: None,
                explain: true,
                tree: None,
            });
            return Some(after);
        }
        // MySQL's tree formats from MariaDB's JSON.
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
                checks: Vec::new(),
                colls: Vec::new(),
                trigs: Vec::new(),
                language: None,
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
        let has = |w: &[u8]| self.sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
        // A check's clause, as MySQL prints it. Clients alias
        // it (Rails: `cc.check_clause AS expression`, whose metadata names
        // no table), so in a statement over `CHECK_CONSTRAINTS` its one
        // long text column is taken for the clause.
        let over_checks = has(b"CHECK_CONSTRAINTS");
        let checks: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                (c.name.eq_ignore_ascii_case(b"CHECK_CLAUSE")
                    && c.table.eq_ignore_ascii_case(b"CHECK_CONSTRAINTS"))
                    || (over_checks
                        && c.charset != 63
                        && matches!(
                            c.col_type,
                            sprig_proto_mysql::resultset::column_type::BLOB
                                | sprig_proto_mysql::resultset::column_type::MEDIUM_BLOB
                                | sprig_proto_mysql::resultset::column_type::LONG_BLOB
                        ))
            })
            .map(|(i, _)| i)
            .collect();
        let exprs: Vec<usize> = after
            .iter()
            .enumerate()
            .filter(|(i, c)| {
                (c.name.eq_ignore_ascii_case(b"expression")
                    && (show || c.table.eq_ignore_ascii_case(b"STATISTICS")))
                    || checks.contains(i)
            })
            .map(|(i, _)| i)
            .collect();
        let originals = (show && has(b"CREATE"))
            || has(b"ROUTINE")
            || has(b"TRIGGER")
            || has(b"EVENT")
            || has(b"PROC");
        let colls: Vec<usize> = if show {
            after
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    [
                        &b"Collation"[..],
                        b"collation_connection",
                        b"Database Collation",
                        b"Default collation",
                        b"Create Database",
                    ]
                    .iter()
                    .any(|n| c.name.eq_ignore_ascii_case(n))
                })
                .map(|(i, _)| i)
                .collect()
        } else {
            Vec::new()
        };
        let trigs: Vec<usize> = if show {
            after
                .iter()
                .enumerate()
                .filter(|(_, c)| c.name.eq_ignore_ascii_case(b"SQL Original Statement"))
                .map(|(i, _)| i)
                .collect()
        } else {
            Vec::new()
        };
        let language = if show && format == RowFormat::Text {
            routine_status_language_at(after)
        } else {
            None
        };
        let widened = fixed.is_some() && format == RowFormat::Binary;
        if widened
            || !json.is_empty()
            || !exprs.is_empty()
            || originals
            || !colls.is_empty()
            || !trigs.is_empty()
            || language.is_some()
        {
            self.cur = Some(Rows {
                format,
                before: columns.to_vec(),
                after: after.to_vec(),
                widened,
                json,
                exprs,
                originals,
                checks,
                colls,
                trigs,
                language,
                explain: false,
                tree: None,
            });
        }
        if let Some(at) = language {
            let mut with_language = after.to_vec();
            with_language.insert(at, language_column(&after[at - 1]));
            return Some(with_language);
        }
        fixed
    }
}

/// Where MySQL's `Language` column goes in a `SHOW PROCEDURE STATUS` or
/// `SHOW FUNCTION STATUS` result (after `Type`), when MariaDB's lacks it.
fn routine_status_language_at(columns: &[ColumnDef]) -> Option<usize> {
    let at = |n: &[u8]| columns.iter().position(|c| c.name.eq_ignore_ascii_case(n));
    let ty = at(b"Type")?;
    (at(b"Db") == Some(0)
        && at(b"Name") == Some(1)
        && at(b"Definer") == Some(ty + 1)
        && at(b"Language").is_none())
    .then_some(ty + 1)
}

/// MySQL's `Language` column, shaped like `Type` (a short text column).
fn language_column(like: &ColumnDef) -> ColumnDef {
    let mut c = like.clone();
    c.name = Bytes::from_static(b"Language");
    c.org_name = Bytes::from_static(b"Language");
    c
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
/// text).
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

/// A check's clause (`CHECK_CONSTRAINTS.CHECK_CLAUSE`; 3306's view puts it
/// in parentheses) as MySQL 8.4 prints it, if that changes it: the
/// expression printed MySQL's way, each `'` written `\'` (Rails
/// compares it, `_utf8mb4'x'` included). MariaDB's text when the
/// printer doesn't know a construct.
fn check_value(v: &[u8]) -> Option<Vec<u8>> {
    let t = std::str::from_utf8(v).ok()?;
    let inner = t
        .strip_prefix('(')
        .and_then(|x| x.strip_suffix(')'))
        .filter(|x| sprig_sql::compat::exprprint::mysql_expr(x, "utf8mb4").is_some())
        .unwrap_or(t);
    let p =
        sprig_sql::compat::exprprint::mysql_expr(inner, "utf8mb4").unwrap_or_else(|| t.to_owned());
    let p = p.replace('\'', "\\'").into_bytes();
    (p.as_slice() != v).then_some(p)
}

/// A `SHOW` value holding a collation name (or `SHOW CREATE DATABASE`'s
/// text, `column` names which) with MySQL's names, if that changes it.
fn coll_value(column: &[u8], v: &[u8]) -> Option<Vec<u8>> {
    use sprig_sql::compat::schema;
    let t = std::str::from_utf8(v).ok()?;
    let m = if column.eq_ignore_ascii_case(b"Create Database") {
        schema::mysql_create_database(t)
    } else {
        schema::mysql_collation_value(t).to_owned()
    };
    (m.as_bytes() != v).then(|| m.into_bytes())
}

impl Rows {
    /// The columns whose values may change.
    fn checked(&self) -> Vec<usize> {
        if self.originals {
            (0..self.after.len()).collect()
        } else {
            self.json
                .iter()
                .chain(&self.exprs)
                .chain(&self.colls)
                .chain(&self.trigs)
                .copied()
                .collect()
        }
    }

    /// Column `i`'s value `v` as the client gets it, if it changes.
    fn fix(&self, i: usize, v: &[u8]) -> Option<Vec<u8>> {
        if self.trigs.contains(&i) {
            let restored = (self.originals && sprig_sql::compat::stored::has_original(v))
                .then(|| sprig_sql::compat::stored::restore_original(v))
                .flatten();
            let t = std::str::from_utf8(restored.as_deref().unwrap_or(v)).ok()?;
            return sprig_sql::compat::trigger::mysql_trigger_statement(t)
                .map(String::into_bytes)
                .or(restored);
        }
        if self.originals && sprig_sql::compat::stored::has_original(v) {
            return sprig_sql::compat::stored::restore_original(v);
        }
        if self.colls.contains(&i) {
            return coll_value(&self.after[i].name, v);
        }
        if self.checks.contains(&i) {
            check_value(v)
        } else if self.exprs.contains(&i) {
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
                if self.json.is_empty()
                    && self.exprs.is_empty()
                    && self.colls.is_empty()
                    && self.trigs.is_empty()
                    && self.language.is_none()
                    && !self.originals
                {
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
                if !any && self.language.is_none() {
                    return None;
                }
                let mut out: Vec<Option<&[u8]>> = vals
                    .iter()
                    .zip(&fixed)
                    .map(|(v, f)| f.as_deref().or(*v))
                    .collect();
                if let Some(at) = self.language {
                    out.insert(at.min(out.len()), Some(b"SQL"));
                }
                Some(encode_text_row(out))
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
        // A prepared EXPLAIN's decoded binary rows.
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
            && !(cur.json.is_empty()
                && cur.exprs.is_empty()
                && cur.colls.is_empty()
                && cur.trigs.is_empty()
                && cur.language.is_none()
                && !cur.originals)
        {
            let mut fixed: Vec<Option<Vec<u8>>> = vec![None; vals.len()];
            for i in cur.checked() {
                if let Some(Some(v)) = vals.get(i) {
                    fixed[i] = cur.fix(i, v);
                }
            }
            if fixed.iter().any(Option::is_some) || cur.language.is_some() {
                let mut out: Vec<Option<&[u8]>> = vals
                    .iter()
                    .zip(&fixed)
                    .map(|(v, f)| f.as_deref().or(*v))
                    .collect();
                if let Some(at) = cur.language {
                    out.insert(at.min(out.len()), Some(b"SQL"));
                }
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
    // A prepared EXPLAIN gets MySQL's twelve columns too (PDO compares it
    // with the emulated one).
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
    use super::{check_value, coll_value, expr_value};

    #[test]
    fn functional_part_expressions_as_mysql_prints_them() {
        // MySQL 8.4.11's `STATISTICS.EXPRESSION`
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

    #[test]
    fn checks_and_collations_as_mysql_prints_them() {
        // MySQL 8.4.11's `CHECK_CONSTRAINTS.CHECK_CLAUSE` for
        // the gateway's (MariaDB 11.8.9's) text.
        let c = |v: &str| check_value(v.as_bytes()).map(|b| String::from_utf8(b).unwrap());
        assert_eq!(
            c("(`name` <> 'forbidden_string')").as_deref(),
            Some("(`name` <> _utf8mb4\\'forbidden_string\\')")
        );
        assert_eq!(
            c("(`a` * 2 + 1 > 0)").as_deref(),
            Some("(((`a` * 2) + 1) > 0)")
        );
        assert_eq!(c("(((`a` * 2) + 1) > 0)"), None);
        // `SHOW` values.
        let v = |col: &str, v: &str| {
            coll_value(col.as_bytes(), v.as_bytes()).map(|b| String::from_utf8(b).unwrap())
        };
        assert_eq!(
            v("Collation", "utf8mb4_uca1400_ai_ci").as_deref(),
            Some("utf8mb4_0900_ai_ci")
        );
        assert_eq!(v("Collation", "utf8mb4_bin"), None);
        assert_eq!(v("Collation", "A"), None);
        assert_eq!(
            v("Database Collation", "utf8mb4_uca1400_nopad_ai_ci").as_deref(),
            Some("utf8mb4_0900_ai_ci")
        );
        assert!(
            v(
                "Create Database",
                "CREATE DATABASE `d` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci */"
            )
            .is_some_and(|t| t.ends_with("utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */"))
        );
    }
}
