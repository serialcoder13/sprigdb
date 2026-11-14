//! Result mapping: the [`ResultMapper`] that relays and rewrites
//! resultsets.
//!
//! A forwarded statement comes back through one [`ResultMapper`]:
//!
//! ```text
//! columns ─► mapper.column_defs(&mut defs)       every mode: schema → logical
//! rows:
//!   needs_rows() == false ─► relay row packets as bytes
//!   needs_rows() == true  ─► for each row: mapper.row(row, &mut out); send out
//!                            at the terminator: mapper.finish(&mut out); send out
//! ```
//!
//! | Mode | Used for | Rows |
//! |---|---|---|
//! | relay | ordinary statements | raw |
//! | show | the `SHOW` family | per-column roles: map schemas, drop hidden and foreign rows, filter definitions; then every value is scrubbed |
//! | scrub | `information_schema` queries, other `SHOW`s | every value through [`super::output::map_bytes`]: account columns (`DEFINER`, `GRANTEE`) and any physical name left |
//!
//! Rows stream one by one, except `SHOW INDEX`, which holds the rows of
//! one index (consecutive in MariaDB's output) until it knows whether any
//! of its columns is hidden.

use bytes::Bytes;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::Persona;
use sprig_proto_mysql::resultset::{decode_text_row, encode_text_row};

use super::hide::{Visibility, filter_create_table};
use super::map::{NameMap, Purpose};
use super::output::{OutputCtx, map_bytes, map_column_def, map_text};
use super::rewrite::unrewrite;
use super::show::ShowKind;
use super::stored;

/// A text row; `None` is SQL NULL.
pub type TextRow = Vec<Option<Bytes>>;

/// A resultset built in the gateway.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalAnswer {
    /// Column definitions, already logical.
    pub columns: Vec<ColumnDef>,
    /// Rows.
    pub rows: Vec<TextRow>,
}

impl LocalAnswer {
    /// Sets the character set of the text columns (every column whose
    /// charset is not `binary`), for the session's
    /// `character_set_results`.
    pub fn set_text_charset(&mut self, charset: u16) {
        for c in &mut self.columns {
            if c.charset != 63 {
                c.charset = charset;
            }
        }
    }
}

/// What a column of a `SHOW` result holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A schema name: mapped; rows of schemas that are not one of the
    /// tenant's representative schemas are dropped.
    SchemaFilter,
    /// A schema name: mapped, never dropped.
    SchemaMap,
    /// A table, view, routine, trigger or event name: hidden rows dropped.
    Object,
    /// A column name: hidden rows dropped.
    Column,
    /// `SHOW INDEX`: the table, for grouping.
    IndexTable,
    /// `SHOW INDEX`: the index name.
    IndexName,
    /// `SHOW INDEX`: a column of the index.
    IndexColumn,
    /// SQL text with schema names in it.
    Definition,
    /// `SHOW CREATE TABLE` text: the line filter, then as a definition.
    CreateTable,
    /// A warning or error message.
    Message,
}

/// The columns each `SHOW` maps, by name (ignoring case).
fn spec(kind: ShowKind) -> &'static [(&'static str, Role)] {
    use Role::{
        CreateTable, Definition, IndexColumn, IndexName, IndexTable, Message, Object, SchemaFilter,
        SchemaMap,
    };
    match kind {
        // The Tables_in_ column is found by prefix.
        ShowKind::Tables | ShowKind::TableStatus => &[("Name", Object)],
        ShowKind::Columns => &[("Field", Role::Column)],
        ShowKind::Index => &[
            ("Table", IndexTable),
            ("Key_name", IndexName),
            ("Column_name", IndexColumn),
        ],
        ShowKind::CreateTable | ShowKind::CreateSequence => {
            &[("Create Table", CreateTable), ("Create View", Definition)]
        }
        ShowKind::CreateView => &[("Create View", Definition)],
        ShowKind::CreateProcedure => &[("Create Procedure", Definition)],
        ShowKind::CreateFunction => &[("Create Function", Definition)],
        ShowKind::CreatePackage => &[
            ("Create Package", Definition),
            ("Create Package Body", Definition),
        ],
        ShowKind::CreateTrigger => &[("SQL Original Statement", Definition)],
        ShowKind::CreateEvent => &[("Create Event", Definition)],
        ShowKind::CreateDatabase => &[("Database", SchemaMap), ("Create Database", Definition)],
        ShowKind::Triggers => &[
            ("Trigger", Object),
            ("Table", Object),
            ("Statement", Definition),
        ],
        ShowKind::Events
        | ShowKind::ProcedureStatus
        | ShowKind::FunctionStatus
        | ShowKind::PackageStatus => &[("Db", SchemaFilter), ("Name", Object)],
        ShowKind::OpenTables => &[("Database", SchemaFilter), ("Table", Object)],
        ShowKind::Warnings | ShowKind::Errors => &[("Message", Message)],
        _ => &[],
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Relay,
    Scrub,
    Show(ShowKind),
}

/// Maps one resultset's column definitions and rows (see the module
/// documentation). Built by [`super::plan()`]; owns no references, so the
/// gateway can keep it across awaits. Each call takes the session's
/// [`OutputCtx`].
#[derive(Debug, Clone)]
pub struct ResultMapper {
    mode: Mode,
    vis: Visibility,
    /// The client's persona: on 3306, `SHOW CREATE TABLE` text is spelled
    /// as MySQL 8.4 spells it.
    persona: Persona,
    roles: Vec<Option<Role>>,
    fixed: Option<Vec<ColumnDef>>,
    /// On 3306, the columns holding a stored program's `sql_mode`, shown
    /// in MySQL's names ([`crate::compat::sqlmode::stored_program_view`]).
    sql_mode_cols: Vec<usize>,
    group: Vec<TextRow>,
    foreign: u64,
    hidden: u64,
}

impl ResultMapper {
    fn new(mode: Mode, vis: Visibility) -> Self {
        Self {
            mode,
            vis,
            persona: Persona::MariaDb,
            roles: Vec::new(),
            fixed: None,
            sql_mode_cols: Vec::new(),
            group: Vec::new(),
            foreign: 0,
            hidden: 0,
        }
    }

    /// Rows pass through as bytes; only column definitions are mapped.
    #[must_use]
    pub fn relay() -> Self {
        Self::new(Mode::Relay, Visibility::none())
    }

    /// Every value is scrubbed of physical and account names.
    #[must_use]
    pub fn scrub() -> Self {
        Self::new(Mode::Scrub, Visibility::none())
    }

    /// The mapper for a `SHOW` statement's result.
    #[must_use]
    pub fn show(kind: ShowKind, vis: Visibility) -> Self {
        Self::new(Mode::Show(kind), vis)
    }

    /// With column definitions that replace the backend's (same count),
    /// for answers MariaDB computes in a different shape (`SHOW DATABASES
    /// WHERE`).
    #[must_use]
    pub fn with_columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.fixed = Some(columns);
        self
    }

    /// With the client's persona (MariaDB's by default).
    #[must_use]
    pub fn with_persona(mut self, persona: Persona) -> Self {
        self.persona = persona;
        self
    }

    /// Sets the client's persona.
    pub fn set_persona(&mut self, persona: Persona) {
        self.persona = persona;
    }

    /// The `SHOW` kind, for a show mapper.
    #[must_use]
    pub fn show_kind(&self) -> Option<ShowKind> {
        match self.mode {
            Mode::Show(k) => Some(k),
            _ => None,
        }
    }

    /// Whether rows must go through [`ResultMapper::row`]; `false` means
    /// they are relayed raw.
    #[must_use]
    pub fn needs_rows(&self) -> bool {
        self.mode != Mode::Relay
    }

    /// Foreign names replaced by `?` so far (a security event and
    /// `sprig_names_foreign_names_total`).
    #[must_use]
    pub fn foreign_names(&self) -> u64 {
        self.foreign
    }

    /// Rows dropped so far (`sprig_names_hidden_rows_total`).
    #[must_use]
    pub fn hidden_rows(&self) -> u64 {
        self.hidden
    }

    /// Maps the column definitions in place, and learns which columns
    /// hold names.
    pub fn column_defs(&mut self, defs: &mut [ColumnDef], ctx: &OutputCtx<'_>) {
        if let Some(fixed) = &self.fixed
            && fixed.len() == defs.len()
        {
            defs.clone_from_slice(fixed);
        }
        for d in defs.iter_mut() {
            self.foreign += map_column_def(d, ctx) as u64;
        }
        if self.persona == Persona::MySql84 && self.mode != Mode::Relay {
            self.sql_mode_cols = defs
                .iter()
                .enumerate()
                .filter(|(_, d)| {
                    let org = if d.org_name.is_empty() {
                        &d.name
                    } else {
                        &d.org_name
                    };
                    org.eq_ignore_ascii_case(b"sql_mode")
                })
                .map(|(i, _)| i)
                .collect();
        }
        let Mode::Show(kind) = self.mode else {
            return;
        };
        let spec = spec(kind);
        self.roles = defs
            .iter()
            .map(|d| {
                let name = String::from_utf8_lossy(&d.name);
                if kind == ShowKind::Tables && name.starts_with("Tables_in_") {
                    return Some(Role::Object);
                }
                spec.iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&name))
                    .map(|&(_, r)| r)
            })
            .collect();
    }

    /// Maps one row, pushing what the client gets (zero or more rows) to
    /// `out`.
    pub fn row(&mut self, row: TextRow, ctx: &OutputCtx<'_>, out: &mut Vec<TextRow>) {
        match self.mode {
            Mode::Relay => out.push(row),
            Mode::Scrub => out.push(self.scrub_row(row, ctx)),
            Mode::Show(ShowKind::Index) => {
                if let Some(last) = self.group.last()
                    && self.index_key(last) != self.index_key(&row)
                {
                    self.flush_index(ctx, out);
                }
                self.group.push(row);
            }
            Mode::Show(_) => {
                if let Some(r) = self.show_row(row, ctx) {
                    out.push(r);
                } else {
                    self.hidden += 1;
                }
            }
        }
    }

    /// Ends the rows, pushing anything held back.
    pub fn finish(&mut self, ctx: &OutputCtx<'_>, out: &mut Vec<TextRow>) {
        if !self.group.is_empty() {
            self.flush_index(ctx, out);
        }
    }

    /// [`ResultMapper::row`] on a text-row packet: decodes it, maps it and
    /// encodes what the client gets.
    ///
    /// # Errors
    ///
    /// A malformed row.
    pub fn row_packet(
        &mut self,
        payload: &Bytes,
        columns: usize,
        ctx: &OutputCtx<'_>,
        out: &mut Vec<Bytes>,
    ) -> sprig_proto_mysql::ProtoResult<()> {
        let vals = decode_text_row(payload, columns)?;
        let row: TextRow = vals
            .into_iter()
            .map(|v| v.map(|b| payload.slice_ref(b)))
            .collect();
        let mut rows = Vec::new();
        self.row(row, ctx, &mut rows);
        out.extend(
            rows.iter()
                .map(|r| encode_text_row(r.iter().map(Option::as_deref))),
        );
        Ok(())
    }

    /// [`ResultMapper::finish`] as encoded row packets.
    pub fn finish_packets(&mut self, ctx: &OutputCtx<'_>, out: &mut Vec<Bytes>) {
        let mut rows = Vec::new();
        self.finish(ctx, &mut rows);
        out.extend(
            rows.iter()
                .map(|r| encode_text_row(r.iter().map(Option::as_deref))),
        );
    }

    /// Maps a whole buffered resultset: definitions in place, rows
    /// returned.
    pub fn map_all(
        &mut self,
        defs: &mut [ColumnDef],
        rows: impl IntoIterator<Item = TextRow>,
        ctx: &OutputCtx<'_>,
    ) -> Vec<TextRow> {
        self.column_defs(defs, ctx);
        let mut out = Vec::new();
        for r in rows {
            self.row(r, ctx, &mut out);
        }
        self.finish(ctx, &mut out);
        out
    }

    fn scrub_row(&mut self, mut row: TextRow, ctx: &OutputCtx<'_>) -> TextRow {
        for &i in &self.sql_mode_cols {
            if let Some(Some(v)) = row.get_mut(i) {
                let shown =
                    crate::compat::sqlmode::stored_program_view(&String::from_utf8_lossy(v));
                *v = Bytes::from(shown);
            }
        }
        for v in row.iter_mut().flatten() {
            // Lookups in stored bodies read back as the calls the client
            // wrote.
            if let std::borrow::Cow::Owned(o) = stored::restore(v) {
                *v = Bytes::from(o);
            }
            let m = map_bytes(v, ctx);
            if let std::borrow::Cow::Owned(o) = m.text {
                self.foreign += m.foreign as u64;
                *v = Bytes::from(o);
            }
        }
        row
    }

    fn role_value(&self, row: &TextRow, role: Role) -> Option<String> {
        let i = self.roles.iter().position(|r| *r == Some(role))?;
        row.get(i)?
            .as_deref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }

    fn index_key(&self, row: &TextRow) -> (Option<String>, Option<String>) {
        (
            self.role_value(row, Role::IndexTable),
            self.role_value(row, Role::IndexName),
        )
    }

    fn flush_index(&mut self, ctx: &OutputCtx<'_>, out: &mut Vec<TextRow>) {
        let group = std::mem::take(&mut self.group);
        let name = group
            .first()
            .and_then(|r| self.role_value(r, Role::IndexName))
            .unwrap_or_default();
        let cols: Vec<String> = group
            .iter()
            .filter_map(|r| self.role_value(r, Role::IndexColumn))
            .collect();
        if self.vis.hides_index(&name, cols.iter().map(String::as_str)) {
            self.hidden += group.len() as u64;
            return;
        }
        for r in group {
            let r = self.scrub_row(r, ctx);
            out.push(r);
        }
    }

    fn show_row(&mut self, mut row: TextRow, ctx: &OutputCtx<'_>) -> Option<TextRow> {
        for (i, cell) in row.iter_mut().enumerate() {
            let Some(role) = self.roles.get(i).copied().flatten() else {
                continue;
            };
            let Some(v) = cell.as_ref() else {
                continue;
            };
            let s = String::from_utf8_lossy(v);
            let new: Option<String> = match role {
                Role::SchemaFilter => match representative_logical(ctx.map, &s) {
                    Some(l) => Some(l),
                    None => return None,
                },
                Role::SchemaMap => ctx.map.logical(&s).map(|d| d.name.clone()),
                Role::Object => {
                    if self.vis.hides_object(&s) {
                        return None;
                    }
                    None
                }
                Role::Column => {
                    if self.vis.hides_column(&s) {
                        return None;
                    }
                    None
                }
                Role::Definition => Some(self.definition(&s, ctx)),
                Role::CreateTable => {
                    // 3306: functional key parts as MySQL writes them,
                    // before their hidden columns are filtered.
                    let fx;
                    let src: &str = if self.persona == Persona::MySql84 {
                        fx = crate::compat::schema::mysql_functional_parts(&s);
                        &fx
                    } else {
                        &s
                    };
                    let mut filtered = filter_create_table(src, &self.vis);
                    if self.persona == Persona::MySql84 {
                        filtered = crate::compat::schema::mysql_create_table(&filtered);
                    }
                    Some(self.definition(&filtered, ctx))
                }
                Role::Message => {
                    let (m, f) = map_text(&s, ctx);
                    self.foreign += f as u64;
                    Some(m.into_owned())
                }
                Role::IndexTable | Role::IndexName | Role::IndexColumn => None,
            };
            if let Some(n) = new {
                *cell = Some(Bytes::from(n));
            }
        }
        Some(self.scrub_row(row, ctx))
    }

    fn definition(&mut self, text: &str, ctx: &OutputCtx<'_>) -> String {
        let text = stored::restore_str(text);
        // A definer routine's wrapper and stored code pointed at row-level
        // security's policy views read as written.
        let text = match crate::rls::definer::unwrap(&text) {
            std::borrow::Cow::Owned(o) => std::borrow::Cow::Owned(o),
            std::borrow::Cow::Borrowed(_) => text,
        };
        let text = match crate::rls::views::unswap(&text) {
            std::borrow::Cow::Owned(o) => std::borrow::Cow::Owned(o),
            std::borrow::Cow::Borrowed(_) => text,
        };
        // Lock names scoped inside stored code read as written.
        let unscoped = crate::compat::locks::unscope(&text);
        let text = unscoped.as_deref().unwrap_or(&text);
        let un = unrewrite(text.as_bytes(), ctx.map, &ctx.lex);
        let m = map_bytes(&un, ctx);
        self.foreign += m.foreign as u64;
        String::from_utf8_lossy(&m.text).into_owned()
    }
}

/// The logical database for `schema` when it is the representative schema
/// (`Purpose::Metadata`) of one of the tenant's databases, so a database
/// with several shards is listed once.
fn representative_logical(map: &dyn NameMap, schema: &str) -> Option<String> {
    let db = map.logical(schema)?;
    let reps = map.physical(&db.name, Purpose::Metadata).ok()?;
    reps.0
        .iter()
        .any(|s| s.to_string() == schema)
        .then(|| db.name.clone())
}

#[cfg(test)]
mod tests;
