//! The table data view and structure tabs (the Studio and TUI redesign),
//! the terminal form of Studio's table page.
//!
//! - **Data.** Keyset pages of 100 rows in primary-key order, under a filter
//!   (a WHERE clause, typed with `/`, or `=` for "this column equals this
//!   cell"). Edits are staged: changed cells amber, new rows green, deleted
//!   rows red and struck through. `a` applies them in one transaction after
//!   showing the statements, `r` resets, `y` copies them as SQL.
//! - **Concurrency.** Optimistic, as in Studio: each UPDATE or DELETE names its row by every column as
//!   read (`<=>`, NULL-safe), so a row changed meanwhile matches nothing.
//!   Any such conflict rolls the whole transaction back and shows the row as
//!   it is now; `t` takes the current values and keeps the edits.
//! - **Structure.** Columns, Indexes, Relations, Triggers, Policies,
//!   Inheritance, Shards and DDL, from the same statements and APIs as
//!   Studio's tabs (views: Data, Columns, Policies and DDL).

use std::collections::{BTreeMap, BTreeSet};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell as TCell, Paragraph, Row, Table, Wrap};
use serde_json::Value;
use unicode_width::UnicodeWidthStr;

use crate::model::{Cell, Column, ResultSet};
use crate::output::cell_text;
use crate::tui::icons::{self, Icon};
use crate::tui::theme;

/// Rows per page.
pub const PAGE: usize = 100;
const MAX_COL: usize = 32;

/// A sub-tab of the table view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Sub {
    /// Rows, filter and staged edits.
    #[default]
    Data,
    /// The catalog's columns.
    Columns,
    /// `SHOW INDEX`.
    Indexes,
    /// Foreign keys out of and into the table.
    Relations,
    /// SQL triggers.
    Triggers,
    /// Row-level security policies.
    Policies,
    /// Inheritance.
    Inheritance,
    /// The table group and shards.
    Shards,
    /// `SHOW CREATE`.
    Ddl,
}

const TABLE_SUBS: [Sub; 9] = [
    Sub::Data,
    Sub::Columns,
    Sub::Indexes,
    Sub::Relations,
    Sub::Triggers,
    Sub::Policies,
    Sub::Inheritance,
    Sub::Shards,
    Sub::Ddl,
];
const VIEW_SUBS: [Sub; 4] = [Sub::Data, Sub::Columns, Sub::Policies, Sub::Ddl];

impl Sub {
    /// The tab's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Sub::Data => "Data",
            Sub::Columns => "Columns",
            Sub::Indexes => "Indexes",
            Sub::Relations => "Relations",
            Sub::Triggers => "Triggers",
            Sub::Policies => "Policies",
            Sub::Inheritance => "Inheritance",
            Sub::Shards => "Shards",
            Sub::Ddl => "DDL",
        }
    }

    /// The short name, for narrow terminals.
    #[must_use]
    pub fn short(self) -> &'static str {
        match self {
            Sub::Data => "Data",
            Sub::Columns => "Cols",
            Sub::Indexes => "Idx",
            Sub::Relations => "Rel",
            Sub::Triggers => "Trig",
            Sub::Policies => "Pol",
            Sub::Inheritance => "Inh",
            Sub::Shards => "Shards",
            Sub::Ddl => "DDL",
        }
    }
}

/// A column as the catalog snapshot describes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColInfo {
    /// Its name.
    pub name: String,
    /// Its type.
    pub type_name: String,
    /// It takes NULL.
    pub nullable: bool,
    /// `PRI`, `UNI`, `MUL` or empty.
    pub key: String,
    /// The default, as text.
    pub default: Option<String>,
    /// `auto_increment` and the like.
    pub extra: String,
    /// A generated column (never written).
    pub generated: bool,
}

/// The columns of `table` in a database's catalog snapshot.
#[must_use]
pub fn col_infos(snapshot: &Value, table: &str) -> Vec<ColInfo> {
    let s = |c: &Value, k: &str| c.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
    snapshot
        .get("columns")
        .and_then(|m| m.get(table))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|c| ColInfo {
            name: s(c, "name"),
            type_name: s(c, "type"),
            nullable: c.get("nullable").and_then(Value::as_bool).unwrap_or(true),
            key: s(c, "key"),
            default: c.get("default").and_then(Value::as_str).map(str::to_owned),
            extra: s(c, "extra"),
            generated: c.get("generated").and_then(Value::as_bool).unwrap_or(false),
        })
        .collect()
}

// ---- SQL ----

/// `` `name` ``.
#[must_use]
pub fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// A MySQL string literal, or `NULL`.
#[must_use]
pub fn value(v: &Cell) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(s) => format!(
            "'{}'",
            s.replace('\\', "\\\\")
                .replace('\'', "''")
                .replace('\0', "\\0")
        ),
    }
}

fn approx(type_name: &str) -> bool {
    let t = type_name.to_ascii_uppercase();
    ["FLOAT", "DOUBLE", "REAL", "DECIMAL", "NEWDECIMAL"]
        .iter()
        .any(|p| t.starts_with(p))
}

/// One staged change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// Cells of a read row.
    Update {
        /// The row in the page.
        row: usize,
        /// The row as read.
        original: Vec<Cell>,
        /// New values by column.
        values: BTreeMap<usize, Cell>,
    },
    /// A read row.
    Delete {
        /// The row in the page.
        row: usize,
        /// The row as read.
        original: Vec<Cell>,
    },
    /// A new row.
    Insert {
        /// Its place after the read rows.
        row: usize,
        /// Its values by column (unset columns take their default).
        values: BTreeMap<usize, Cell>,
    },
}

/// A statement to apply, and how to read its row again after a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyStmt {
    /// The statement.
    pub sql: String,
    /// The row it changes.
    pub row: usize,
    /// For updates and deletes: the row by its key, as it is now.
    pub reread: Option<String>,
}

/// A row changed by someone else since it was read: its place and its
/// values now (`None`: deleted).
pub type Conflict = (usize, Option<Vec<Cell>>);

/// What a table query is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Purpose {
    /// A page of rows.
    Page,
    /// A structure tab.
    Structure(Sub),
}

/// What the view needs from the app after a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableCmd {
    /// Nothing.
    None,
    /// Run this for a page or a structure tab.
    Query(Purpose, String),
    /// Read `/v1/shards`.
    Shards,
    /// Ask for the filter (the current one).
    EditFilter(String),
    /// Ask for a cell's value (the current one; `None` for NULL).
    EditCell(Cell),
    /// Show these and apply them on a yes.
    Apply(Vec<ApplyStmt>),
    /// Put this on the clipboard.
    Copy(String, usize),
    /// A line for the status bar.
    Status(String),
}

/// One table's view.
#[derive(Debug, Clone, Default)]
pub struct TableView {
    /// The database.
    pub db: String,
    /// The table or view.
    pub table: String,
    /// A view: read-only, fewer tabs.
    pub is_view: bool,
    /// The shown tab.
    pub sub: Sub,
    /// The catalog's columns, once read.
    pub cols: Vec<ColInfo>,
    /// The WHERE clause, empty for none.
    pub filter: String,
    /// The key each page shown so far starts after; the last is this page's.
    pub starts: Vec<Option<Vec<Cell>>>,
    /// Another page follows.
    pub has_more: bool,
    /// The page's columns.
    pub columns: Vec<Column>,
    /// The page's rows as read.
    pub rows: Vec<Vec<Cell>>,
    /// Staged values of read rows, by row and column.
    pub edits: BTreeMap<usize, BTreeMap<usize, Cell>>,
    /// Read rows staged for deletion.
    pub deleted: BTreeSet<usize>,
    /// Staged new rows.
    pub inserted: Vec<Vec<Cell>>,
    /// Selected row (read rows, then new ones).
    pub row: usize,
    /// Selected column.
    pub col: usize,
    /// First visible row.
    pub top: usize,
    /// A page or structure query runs.
    pub loading: bool,
    /// The last error.
    pub error: Option<String>,
    /// Conflicts from the last apply.
    pub conflicts: Vec<Conflict>,
    /// The shown structure tab's result.
    pub other: Option<(Sub, Result<ResultSet, String>)>,
    /// Selected row of a structure tab.
    pub srow: usize,
}

impl TableView {
    /// A view of `db`.`table`, before anything is read.
    #[must_use]
    pub fn new(db: &str, table: &str, is_view: bool) -> Self {
        Self {
            db: db.to_owned(),
            table: table.to_owned(),
            is_view,
            starts: vec![None],
            loading: true,
            ..Self::default()
        }
    }

    /// The tabs this object has.
    #[must_use]
    pub fn subs(&self) -> &'static [Sub] {
        if self.is_view {
            &VIEW_SUBS
        } else {
            &TABLE_SUBS
        }
    }

    fn target(&self) -> String {
        format!("{}.{}", ident(&self.db), ident(&self.table))
    }

    /// The primary key's columns, in column order.
    #[must_use]
    pub fn key_names(&self) -> Vec<String> {
        self.cols
            .iter()
            .filter(|c| c.key == "PRI")
            .map(|c| c.name.clone())
            .collect()
    }

    fn key_idx(&self) -> Vec<usize> {
        self.key_names()
            .iter()
            .filter_map(|k| self.columns.iter().position(|c| &c.name == k))
            .collect()
    }

    /// Rows can be changed: a table with a primary key.
    #[must_use]
    pub fn editable(&self) -> bool {
        !self.is_view && !self.key_names().is_empty()
    }

    /// Columns that are never written: generated, `_sprig_*`, binary.
    #[must_use]
    pub fn read_only(&self) -> BTreeSet<usize> {
        let generated: BTreeSet<&str> = self
            .cols
            .iter()
            .filter(|c| c.generated)
            .map(|c| c.name.as_str())
            .collect();
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.binary || c.name.starts_with("_sprig_") || generated.contains(c.name.as_str())
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// The query for the current page (one row more than shown, to know
    /// whether another page follows).
    #[must_use]
    pub fn page_sql(&self) -> String {
        let after = self.starts.last().cloned().flatten();
        let keys = self.key_names();
        let mut conds: Vec<String> = Vec::new();
        if !self.filter.trim().is_empty() {
            conds.push(format!("({})", self.filter.trim()));
        }
        let limit = PAGE + 1;
        if keys.is_empty() {
            let w = if conds.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", conds.join(" AND "))
            };
            return format!("SELECT * FROM {}{w} LIMIT {limit}", self.target());
        }
        let order = keys.iter().map(|k| ident(k)).collect::<Vec<_>>().join(", ");
        if let Some(after) = after {
            let (lhs, rhs) = if keys.len() == 1 {
                (ident(&keys[0]), value(&after.first().cloned().flatten()))
            } else {
                (
                    format!("({order})"),
                    format!(
                        "({})",
                        after.iter().map(value).collect::<Vec<_>>().join(", ")
                    ),
                )
            };
            conds.push(format!("{lhs} > {rhs}"));
        }
        let w = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        format!(
            "SELECT * FROM {}{w} ORDER BY {order} LIMIT {limit}",
            self.target()
        )
    }

    /// Takes a page of rows; staged changes are dropped.
    pub fn set_page(&mut self, set: ResultSet) {
        self.loading = false;
        self.error = None;
        self.has_more = set.rows.len() > PAGE;
        self.columns = set.columns;
        self.rows = set.rows;
        self.rows.truncate(PAGE);
        self.reset();
        self.row = 0;
        self.top = 0;
        self.col = self.col.min(self.columns.len().saturating_sub(1));
    }

    /// Drops every staged change.
    pub fn reset(&mut self) {
        self.edits.clear();
        self.deleted.clear();
        self.inserted.clear();
        self.conflicts.clear();
        self.row = self.row.min(self.rows.len().saturating_sub(1));
    }

    /// Read rows and new rows.
    #[must_use]
    pub fn total(&self) -> usize {
        self.rows.len() + self.inserted.len()
    }

    /// A cell's value with staged edits.
    #[must_use]
    pub fn cell(&self, r: usize, c: usize) -> Cell {
        if r >= self.rows.len() {
            return self
                .inserted
                .get(r - self.rows.len())
                .and_then(|row| row.get(c))
                .cloned()
                .flatten();
        }
        if let Some(v) = self.edits.get(&r).and_then(|e| e.get(&c)) {
            return v.clone();
        }
        self.rows
            .get(r)
            .and_then(|row| row.get(c))
            .cloned()
            .flatten()
    }

    /// Stages `v` in a cell; the value as read unstages it.
    pub fn stage(&mut self, r: usize, c: usize, v: Cell) {
        if r >= self.rows.len() {
            if let Some(cell) = self
                .inserted
                .get_mut(r - self.rows.len())
                .and_then(|row| row.get_mut(c))
            {
                *cell = v;
            }
            return;
        }
        let original = self
            .rows
            .get(r)
            .and_then(|row| row.get(c))
            .cloned()
            .flatten();
        let e = self.edits.entry(r).or_default();
        if original == v {
            e.remove(&c);
        } else {
            e.insert(c, v);
        }
        if e.is_empty() {
            self.edits.remove(&r);
        }
    }

    /// Changes staged, counting each row once.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.edits
            .keys()
            .filter(|r| !self.deleted.contains(r))
            .count()
            + self.deleted.len()
            + self.inserted.len()
    }

    /// The staged changes: updates, then deletes, then inserts.
    #[must_use]
    pub fn changes(&self) -> Vec<Change> {
        let mut out = Vec::new();
        for (r, values) in &self.edits {
            if !self.deleted.contains(r) {
                out.push(Change::Update {
                    row: *r,
                    original: self.rows[*r].clone(),
                    values: values.clone(),
                });
            }
        }
        for r in &self.deleted {
            out.push(Change::Delete {
                row: *r,
                original: self.rows[*r].clone(),
            });
        }
        for (i, row) in self.inserted.iter().enumerate() {
            out.push(Change::Insert {
                row: self.rows.len() + i,
                values: row
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| v.is_some())
                    .map(|(c, v)| (c, v.clone()))
                    .collect(),
            });
        }
        out
    }

    /// The row as read, named by every column (NULL-safe).
    fn cond(&self, original: &[Cell]) -> String {
        let key = self.key_idx();
        let mut parts = Vec::new();
        for (i, c) in self.columns.iter().enumerate() {
            let v = value(&original.get(i).cloned().flatten());
            if c.binary {
                // Base64 in the API: compare binary columns of the key only.
                if key.contains(&i) {
                    parts.push(format!("TO_BASE64({}) <=> {v}", ident(&c.name)));
                }
                continue;
            }
            let col = if approx(&c.type_name) {
                format!("CAST({} AS CHAR)", ident(&c.name))
            } else {
                ident(&c.name)
            };
            parts.push(format!("{col} <=> {v}"));
        }
        parts.join(" AND ")
    }

    fn reread(&self, original: &[Cell]) -> String {
        let w = self
            .key_idx()
            .iter()
            .map(|&i| {
                format!(
                    "{} <=> {}",
                    ident(&self.columns[i].name),
                    value(&original.get(i).cloned().flatten())
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        format!("SELECT * FROM {} WHERE {w}", self.target())
    }

    /// The statement for one change.
    #[must_use]
    pub fn statement(&self, ch: &Change) -> ApplyStmt {
        let ro = self.read_only();
        let target = self.target();
        match ch {
            Change::Insert { row, values } => {
                let kept: Vec<(&usize, &Cell)> =
                    values.iter().filter(|(c, _)| !ro.contains(c)).collect();
                let sql = if kept.is_empty() {
                    format!("INSERT INTO {target} () VALUES ()")
                } else {
                    format!(
                        "INSERT INTO {target} ({}) VALUES ({})",
                        kept.iter()
                            .map(|(c, _)| ident(&self.columns[**c].name))
                            .collect::<Vec<_>>()
                            .join(", "),
                        kept.iter()
                            .map(|(_, v)| value(v))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                ApplyStmt {
                    sql,
                    row: *row,
                    reread: None,
                }
            }
            Change::Delete { row, original } => ApplyStmt {
                sql: format!("DELETE FROM {target} WHERE {} LIMIT 1", self.cond(original)),
                row: *row,
                reread: Some(self.reread(original)),
            },
            Change::Update {
                row,
                original,
                values,
            } => {
                let sets = values
                    .iter()
                    .filter(|(c, _)| !ro.contains(c))
                    .map(|(c, v)| format!("{} = {}", ident(&self.columns[*c].name), value(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                ApplyStmt {
                    sql: format!(
                        "UPDATE {target} SET {sets} WHERE {} LIMIT 1",
                        self.cond(original)
                    ),
                    row: *row,
                    reread: Some(self.reread(original)),
                }
            }
        }
    }

    /// Every staged change's statement.
    #[must_use]
    pub fn statements(&self) -> Vec<ApplyStmt> {
        self.changes().iter().map(|c| self.statement(c)).collect()
    }

    /// Takes the current values of conflicting rows as the rows read,
    /// keeping the staged edits; rows deleted meanwhile lose their changes.
    pub fn take_current(&mut self) {
        for (r, now) in std::mem::take(&mut self.conflicts) {
            match now {
                Some(now) if r < self.rows.len() => {
                    self.rows[r] = now;
                    let cols: Vec<(usize, Cell)> = self
                        .edits
                        .remove(&r)
                        .map(|e| e.into_iter().collect())
                        .unwrap_or_default();
                    for (c, v) in cols {
                        self.stage(r, c, v);
                    }
                }
                _ => {
                    self.edits.remove(&r);
                    self.deleted.remove(&r);
                }
            }
        }
    }

    // ---- structure ----

    /// What a structure tab reads: SQL run in the table's database, `/v1/shards`,
    /// or nothing (Columns comes from the catalog).
    #[must_use]
    pub fn structure_cmd(&self, sub: Sub) -> TableCmd {
        let t = value(&Some(self.table.clone()));
        let kcu = "CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, \
                   REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE";
        let q = |s: String| TableCmd::Query(Purpose::Structure(sub), s);
        match sub {
            Sub::Data | Sub::Columns => TableCmd::None,
            Sub::Shards => TableCmd::Shards,
            Sub::Indexes => q(format!("SHOW INDEX FROM {}", self.target())),
            Sub::Relations => q(format!(
                "SELECT 'out' AS direction, {kcu} WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {t} \
                 AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION;\n\
                 SELECT 'in' AS direction, {kcu} WHERE REFERENCED_TABLE_SCHEMA = DATABASE() \
                 AND REFERENCED_TABLE_NAME = {t} ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"
            )),
            Sub::Triggers => q(format!(
                "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT \
                 FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = DATABASE() \
                 AND EVENT_OBJECT_TABLE = {t} ORDER BY ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER"
            )),
            Sub::Policies => q(format!("SHOW POLICIES FROM {}", ident(&self.db))),
            Sub::Inheritance => q("SHOW INHERITABLE TABLES".to_owned()),
            Sub::Ddl => q(format!(
                "SHOW CREATE {} {}",
                if self.is_view { "VIEW" } else { "TABLE" },
                self.target()
            )),
        }
    }

    /// The Columns tab, from the catalog.
    #[must_use]
    pub fn columns_set(&self) -> ResultSet {
        ResultSet {
            columns: ["#", "Name", "Type", "Null", "Key", "Default", "Extra"]
                .into_iter()
                .map(Column::named)
                .collect(),
            rows: self
                .cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    vec![
                        Some((i + 1).to_string()),
                        Some(c.name.clone()),
                        Some(c.type_name.clone()),
                        Some(if c.nullable { "yes" } else { "no" }.to_owned()),
                        Some(c.key.clone()),
                        Some(c.default.clone().unwrap_or_else(|| "NULL".to_owned())),
                        Some(if c.generated && c.extra.is_empty() {
                            "generated".to_owned()
                        } else {
                            c.extra.clone()
                        }),
                    ]
                })
                .collect(),
            ..ResultSet::default()
        }
    }

    /// A structure tab's result from what its statements returned.
    #[must_use]
    pub fn shape(&self, sub: Sub, sets: Vec<ResultSet>) -> ResultSet {
        let mut tabular = sets.into_iter().filter(|s| !s.columns.is_empty());
        match sub {
            Sub::Relations => {
                let mut out = tabular.next().unwrap_or_default();
                for s in tabular {
                    out.rows.extend(s.rows);
                }
                out
            }
            Sub::Policies => {
                let mut s = tabular.next().unwrap_or_default();
                s.rows
                    .retain(|r| r.get(1).cloned().flatten().as_deref() == Some(&self.table));
                s
            }
            Sub::Inheritance => {
                let s = tabular.next().unwrap_or_default();
                let full = format!("{}.{}", self.db, self.table);
                let at = s.columns.iter().position(|c| c.name == "Table");
                let row = s.rows.iter().find(|r| {
                    at.and_then(|i| r.get(i).cloned().flatten())
                        .is_some_and(|t| t == full || t == self.table)
                });
                fields(row.map_or_else(
                    || {
                        vec![(
                            "Inheritable".to_owned(),
                            "no: its rows belong to this tenant only".to_owned(),
                        )]
                    },
                    |r| {
                        s.columns
                            .iter()
                            .zip(r)
                            .map(|(c, v)| (c.name.clone(), v.clone().unwrap_or_default()))
                            .collect()
                    },
                ))
            }
            Sub::Ddl => {
                let s = tabular.next().unwrap_or_default();
                let i = s
                    .columns
                    .iter()
                    .position(|c| c.name.to_ascii_lowercase().starts_with("create "))
                    .unwrap_or(1);
                let text = s
                    .rows
                    .first()
                    .and_then(|r| r.get(i))
                    .cloned()
                    .flatten()
                    .unwrap_or_default();
                ResultSet {
                    columns: vec![Column::named("DDL")],
                    rows: text.lines().map(|l| vec![Some(l.to_owned())]).collect(),
                    ..ResultSet::default()
                }
            }
            _ => tabular.next().unwrap_or_default(),
        }
    }

    /// The Shards tab from `/v1/shards`.
    #[must_use]
    pub fn shards_set(&self, v: &Value) -> ResultSet {
        let s = |x: &Value, k: &str| match x.get(k) {
            Some(Value::String(t)) => t.clone(),
            Some(Value::Null) | None => String::new(),
            Some(o) => o.to_string(),
        };
        let found = v
            .get("groups")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find_map(|g| {
                let m = g.get("members")?.as_array()?.iter().find(|m| {
                    m.get("db").and_then(Value::as_str) == Some(&self.db)
                        && m.get("table").and_then(Value::as_str) == Some(&self.table)
                })?;
                Some((g, m))
            });
        let Some((g, m)) = found else {
            return fields(vec![(
                "Sharded".to_owned(),
                "no: the table lives in one shard with the rest of its database".to_owned(),
            )]);
        };
        // Studio's keyText: the key columns (this table's, else the group
        // root's) and the key's classes from `key_spec`.
        let cols_of = |x: &Value| {
            x.get("key_columns")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        };
        let mut cols = cols_of(m);
        if cols.is_empty() {
            cols = g
                .get("members")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|x| x.get("routing").and_then(Value::as_str) == Some("columns"))
                .map(cols_of)
                .find(|c| !c.is_empty())
                .unwrap_or_default();
        }
        let classes = g
            .get("key_spec")
            .and_then(|k| k.get("parts"))
            .and_then(Value::as_array)
            .map(|p| {
                p.iter()
                    .map(|x| x.get("class").and_then(Value::as_str).unwrap_or("?"))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let key = match (cols.is_empty(), classes.is_empty()) {
            (true, true) if s(g, "kind") == "sharded" => "(not chosen yet)".to_owned(),
            (true, true) => "-".to_owned(),
            (_, false) => format!("{} ({classes})", if cols.is_empty() { "?" } else { &cols }),
            (false, true) => cols,
        };
        let shards = g
            .get("map")
            .and_then(|m| m.get("shards"))
            .and_then(Value::as_array)
            .map_or_else(|| "-".to_owned(), |a| a.len().to_string());
        fields(vec![
            ("Table group".to_owned(), s(g, "name")),
            ("Key".to_owned(), key),
            ("Routing".to_owned(), s(m, "routing")),
            ("State".to_owned(), s(g, "state")),
            ("Shards".to_owned(), shards),
        ])
    }

    // ---- keys ----

    /// Handles a key while the view has the focus.
    pub fn key(&mut self, k: KeyEvent) -> TableCmd {
        let subs = self.subs();
        let at = subs.iter().position(|s| *s == self.sub).unwrap_or(0);
        match k.code {
            KeyCode::Char('[' | ']') => {
                let n = subs.len();
                let next = if k.code == KeyCode::Char(']') {
                    (at + 1) % n
                } else {
                    (at + n - 1) % n
                };
                return self.show(subs[next]);
            }
            KeyCode::Char('g') => {
                return if self.sub == Sub::Data {
                    self.reload()
                } else {
                    self.other = None;
                    self.show(self.sub)
                };
            }
            _ => {}
        }
        if self.sub != Sub::Data {
            let n = self
                .other
                .as_ref()
                .and_then(|(_, r)| r.as_ref().ok())
                .map_or_else(
                    || {
                        if self.sub == Sub::Columns {
                            self.cols.len()
                        } else {
                            0
                        }
                    },
                    |s| s.rows.len(),
                );
            self.srow = step(self.srow, n, k.code);
            return TableCmd::None;
        }
        self.data_key(k)
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one arm per key
    fn data_key(&mut self, k: KeyEvent) -> TableCmd {
        let n = self.total();
        match k.code {
            KeyCode::Up
            | KeyCode::Down
            | KeyCode::PageUp
            | KeyCode::PageDown
            | KeyCode::Home
            | KeyCode::End => {
                self.row = step(self.row, n, k.code);
                TableCmd::None
            }
            KeyCode::Left => {
                self.col = self.col.saturating_sub(1);
                TableCmd::None
            }
            KeyCode::Right => {
                self.col = (self.col + 1).min(self.columns.len().saturating_sub(1));
                TableCmd::None
            }
            KeyCode::Char('/') => TableCmd::EditFilter(self.filter.clone()),
            KeyCode::Char('=') => {
                let Some(c) = self.columns.get(self.col) else {
                    return TableCmd::None;
                };
                if self.row >= self.rows.len() || c.binary {
                    return TableCmd::Status("filter by a read, non-binary cell".to_owned());
                }
                let v = self.rows[self.row].get(self.col).cloned().flatten();
                let cond = match v {
                    None => format!("{} IS NULL", ident(&c.name)),
                    some => format!("{} = {}", ident(&c.name), value(&some)),
                };
                let f = if self.filter.trim().is_empty() {
                    cond
                } else {
                    format!("{} AND {cond}", self.filter.trim())
                };
                self.set_filter(&f)
            }
            KeyCode::Char('n') => {
                if self.pending() > 0 {
                    return TableCmd::Status("apply (a) or reset (r) the changes first".into());
                }
                if !self.has_more {
                    return TableCmd::Status("this is the last page".into());
                }
                if self.key_names().is_empty() {
                    return TableCmd::Status(
                        "no primary key: only the first 100 rows can be shown".into(),
                    );
                }
                let key = self.key_idx();
                let last = self.rows.last().cloned().unwrap_or_default();
                self.starts.push(Some(
                    key.iter()
                        .map(|&i| last.get(i).cloned().flatten())
                        .collect(),
                ));
                self.reload()
            }
            KeyCode::Char('p') => {
                if self.pending() > 0 {
                    return TableCmd::Status("apply (a) or reset (r) the changes first".into());
                }
                if self.starts.len() < 2 {
                    return TableCmd::Status("this is the first page".into());
                }
                self.starts.pop();
                self.reload()
            }
            KeyCode::Char('r') => {
                let n = self.pending();
                self.reset();
                TableCmd::Status(format!("reset {n} change{}", plural(n)))
            }
            KeyCode::Char('y') => {
                let s = self.statements();
                if s.is_empty() {
                    return TableCmd::Status("no changes to copy".into());
                }
                let n = s.len();
                TableCmd::Copy(
                    s.into_iter()
                        .map(|s| s.sql + ";")
                        .collect::<Vec<_>>()
                        .join("\n"),
                    n,
                )
            }
            KeyCode::Char('t') if !self.conflicts.is_empty() => {
                self.take_current();
                TableCmd::Status("took the current values; your edits are kept: a to apply".into())
            }
            _ if !self.editable() => match k.code {
                KeyCode::Enter | KeyCode::Char('e' | 'o' | 'd' | 'a') | KeyCode::Delete => {
                    TableCmd::Status(if self.is_view {
                        "a view is read-only here".into()
                    } else {
                        "no primary key: rows can't be named, so they can't be edited".into()
                    })
                }
                _ => TableCmd::None,
            },
            KeyCode::Enter | KeyCode::Char('e') => {
                if self.read_only().contains(&self.col) {
                    return TableCmd::Status(
                        "this column can't be written (generated, binary or internal)".into(),
                    );
                }
                if self.deleted.contains(&self.row) || self.row >= n {
                    return TableCmd::None;
                }
                TableCmd::EditCell(self.cell(self.row, self.col))
            }
            KeyCode::Char('o') => {
                self.inserted.push(vec![None; self.columns.len()]);
                self.row = self.total() - 1;
                TableCmd::Status("new row: Enter edits a cell, a applies".into())
            }
            KeyCode::Char('d') | KeyCode::Delete => {
                if self.row >= self.rows.len() {
                    let i = self.row - self.rows.len();
                    if i < self.inserted.len() {
                        self.inserted.remove(i);
                        self.row = self.row.min(self.total().saturating_sub(1));
                    }
                } else if !self.deleted.remove(&self.row) {
                    self.deleted.insert(self.row);
                }
                TableCmd::None
            }
            KeyCode::Char('a') => {
                let s = self.statements();
                if s.is_empty() {
                    TableCmd::Status("no changes to apply".into())
                } else {
                    TableCmd::Apply(s)
                }
            }
            _ => TableCmd::None,
        }
    }

    /// Sets the filter and reads the first page.
    pub fn set_filter(&mut self, f: &str) -> TableCmd {
        if self.pending() > 0 {
            return TableCmd::Status("apply (a) or reset (r) the changes first".into());
        }
        f.trim().clone_into(&mut self.filter);
        self.starts = vec![None];
        self.reload()
    }

    /// Reads the current page again.
    pub fn reload(&mut self) -> TableCmd {
        self.loading = true;
        TableCmd::Query(Purpose::Page, self.page_sql())
    }

    /// Shows a tab, reading it when needed.
    pub fn show(&mut self, sub: Sub) -> TableCmd {
        self.sub = sub;
        self.srow = 0;
        if matches!(sub, Sub::Data | Sub::Columns) {
            return TableCmd::None;
        }
        if self.other.as_ref().is_some_and(|(s, _)| *s == sub) {
            return TableCmd::None;
        }
        self.other = None;
        self.structure_cmd(sub)
    }

    // ---- drawing ----

    /// Draws the view in `area`.
    pub fn draw(&mut self, f: &mut Frame<'_>, area: Rect, focused: bool) {
        let block = Block::bordered()
            .border_style(if focused {
                theme::focus_border()
            } else {
                theme::border()
            })
            .title(format!(
                " {} {}.{}{} ",
                icons::glyph(Icon::Table),
                self.db,
                self.table,
                if self.is_view { " (view)" } else { "" }
            ));
        let all = block.inner(area);
        f.render_widget(block, area);
        let [tabs_area, inner] =
            Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(all);
        let full: usize = self.subs().iter().map(|s| s.name().len() + 3).sum();
        let short = full > tabs_area.width as usize;
        let mut tabs: Vec<Span<'static>> = Vec::new();
        for (i, s) in self.subs().iter().enumerate() {
            if i > 0 {
                tabs.push(Span::styled("│", theme::border()));
            }
            let st = if *s == self.sub {
                theme::tab_active().add_modifier(Modifier::UNDERLINED)
            } else {
                theme::muted()
            };
            let name = if short { s.short() } else { s.name() };
            tabs.push(Span::styled(format!(" {name} "), st));
        }
        f.render_widget(Paragraph::new(Line::from(tabs)), tabs_area);
        if self.sub == Sub::Data {
            self.draw_data(f, inner);
            return;
        }
        let set = if self.sub == Sub::Columns {
            Ok(self.columns_set())
        } else {
            match &self.other {
                Some((s, r)) if *s == self.sub => r.clone(),
                _ => {
                    f.render_widget(Paragraph::new("loading..."), inner);
                    return;
                }
            }
        };
        match set {
            Err(e) => f.render_widget(
                Paragraph::new(Line::styled(e, theme::error())).wrap(Wrap { trim: false }),
                inner,
            ),
            Ok(s) if s.columns.is_empty() || s.rows.is_empty() => f.render_widget(
                Paragraph::new(Line::styled(
                    format!("no {}", self.sub.name().to_ascii_lowercase()),
                    theme::muted(),
                )),
                inner,
            ),
            Ok(s) => {
                let rows: Vec<Vec<(String, Style)>> = s
                    .rows
                    .iter()
                    .map(|r| {
                        s.columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                (cell_text(&r.get(i).cloned().flatten(), c), Style::new())
                            })
                            .collect()
                    })
                    .collect();
                let (srow, mut top) = (self.srow, 0);
                grid(
                    f,
                    inner,
                    &s.columns,
                    &rows,
                    srow,
                    None,
                    &mut top,
                    &BTreeSet::new(),
                );
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn draw_data(&mut self, f: &mut Frame<'_>, area: Rect) {
        let [filter_area, grid_area, foot_area] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .areas(area);
        let filter = if self.filter.is_empty() {
            Line::from(vec![
                Span::styled("WHERE ", theme::keyword()),
                Span::styled("(no filter) · / to filter · = this cell", theme::muted()),
            ])
        } else {
            Line::from(vec![
                Span::styled("WHERE ", theme::keyword()),
                Span::raw(crate::tui::clip(&self.filter, area.width as usize)),
            ])
        };
        f.render_widget(Paragraph::new(filter), filter_area);
        if let Some(e) = &self.error {
            f.render_widget(
                Paragraph::new(Line::styled(e.clone(), theme::error())).wrap(Wrap { trim: false }),
                grid_area,
            );
        } else if self.columns.is_empty() {
            let t = if self.loading {
                "loading..."
            } else {
                "no columns"
            };
            f.render_widget(Paragraph::new(t), grid_area);
        } else {
            let mut rows: Vec<Vec<(String, Style)>> = Vec::with_capacity(self.total());
            for r in 0..self.total() {
                let new = r >= self.rows.len();
                let gone = self.deleted.contains(&r);
                rows.push(
                    self.columns
                        .iter()
                        .enumerate()
                        .map(|(c, col)| {
                            let edited = self.edits.get(&r).is_some_and(|e| e.contains_key(&c));
                            let st = if gone {
                                theme::deleted()
                            } else if new {
                                theme::added()
                            } else if edited {
                                theme::changed()
                            } else {
                                Style::new()
                            };
                            (cell_text(&self.cell(r, c), col), st)
                        })
                        .collect(),
                );
            }
            let key: BTreeSet<usize> = self.key_idx().into_iter().collect();
            let (row, col) = (self.row, self.col);
            grid(
                f,
                grid_area,
                &self.columns,
                &rows,
                row,
                Some(col),
                &mut self.top,
                &key,
            );
        }
        let pending = self.pending();
        let page = self.starts.len();
        let mut left: Vec<Span<'static>> = Vec::new();
        if !self.conflicts.is_empty() {
            left.push(Span::styled(
                format!(
                    "{} row{} changed since read · t take current values ",
                    self.conflicts.len(),
                    plural(self.conflicts.len())
                ),
                theme::warn(),
            ));
        } else if pending > 0 {
            left.push(Span::styled(
                format!("{pending} change{} pending", plural(pending)),
                theme::warn(),
            ));
            left.push(Span::styled(
                " · a apply · r reset · y copy as SQL",
                theme::muted(),
            ));
        } else {
            left.push(Span::styled(
                format!(
                    "page {page} · {} row{}{}",
                    self.rows.len(),
                    plural(self.rows.len()),
                    if self.has_more { " · n next" } else { "" }
                ),
                theme::muted(),
            ));
            left.push(Span::styled(
                if self.editable() {
                    " · e edit · o add · d delete"
                } else {
                    " · read-only"
                },
                theme::muted(),
            ));
        }
        f.render_widget(Paragraph::new(Line::from(left)), foot_area);
    }

    /// The conflict note for the apply result, for the status bar.
    #[must_use]
    pub fn conflict_note(&self) -> String {
        let shown: Vec<String> = self
            .conflicts
            .iter()
            .take(3)
            .map(|(r, now)| match now {
                None => format!("row {} was deleted", r + 1),
                Some(v) => format!(
                    "row {} is now ({})",
                    r + 1,
                    v.iter()
                        .zip(&self.columns)
                        .map(|(c, col)| crate::tui::clip(&cell_text(c, col), 12))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            })
            .collect();
        format!(
            "nothing applied: {}; t takes the current values",
            shown.join("; ")
        )
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// A two-column Field/Value result.
fn fields(kv: Vec<(String, String)>) -> ResultSet {
    ResultSet {
        columns: vec![Column::named("Field"), Column::named("Value")],
        rows: kv
            .into_iter()
            .map(|(k, v)| vec![Some(k), Some(if v.is_empty() { "-".to_owned() } else { v })])
            .collect(),
        ..ResultSet::default()
    }
}

fn step(at: usize, n: usize, k: KeyCode) -> usize {
    let last = n.saturating_sub(1);
    match k {
        KeyCode::Up => at.saturating_sub(1),
        KeyCode::Down => (at + 1).min(last),
        KeyCode::PageUp => at.saturating_sub(20),
        KeyCode::PageDown => (at + 20).min(last),
        KeyCode::Home => 0,
        KeyCode::End => last,
        _ => at.min(last),
    }
}

/// Draws rows as a grid with the selected row (and cell) marked and key
/// columns in the accent; scrolls `top` to keep the row visible and starts
/// at the selected column when the columns don't all fit.
#[allow(clippy::too_many_arguments)]
fn grid(
    f: &mut Frame<'_>,
    area: Rect,
    columns: &[Column],
    rows: &[Vec<(String, Style)>],
    row: usize,
    col: Option<usize>,
    top: &mut usize,
    key: &BTreeSet<usize>,
) {
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            rows.iter()
                .take(200)
                .filter_map(|r| r.get(i))
                .map(|(t, _)| t.width())
                .chain([c.name.width() + usize::from(key.contains(&i)), 3])
                .max()
                .unwrap_or(3)
                .min(MAX_COL)
        })
        .collect();
    // The first shown column: as far left as lets the selected one fit.
    let avail = area.width as usize;
    let mut first = 0;
    if let Some(c) = col {
        while first < c && widths[first..=c].iter().map(|w| w + 1).sum::<usize>() > avail {
            first += 1;
        }
    }
    let mut shown = Vec::new();
    let mut used = 0;
    for (i, w) in widths.iter().enumerate().skip(first) {
        if used + w + 1 > avail && !shown.is_empty() {
            break;
        }
        used += w + 1;
        shown.push(i);
    }
    let h = (area.height as usize).saturating_sub(1);
    if row < *top {
        *top = row;
    } else if h > 0 && row >= *top + h {
        *top = row + 1 - h;
    }
    let header = Row::new(shown.iter().map(|&i| {
        let name = if key.contains(&i) {
            format!("{}*", columns[i].name)
        } else {
            columns[i].name.clone()
        };
        let st = if key.contains(&i) {
            theme::accent().add_modifier(Modifier::BOLD)
        } else {
            Style::new().add_modifier(Modifier::BOLD)
        };
        TCell::from(crate::tui::clip(&name, widths[i])).style(st)
    }));
    let body = rows
        .iter()
        .enumerate()
        .skip(*top)
        .take(h)
        .map(|(r, cells)| {
            let line = Row::new(shown.iter().map(|&i| {
                let (t, st) = cells.get(i).cloned().unwrap_or_default();
                let mut st = st;
                if r == row && col == Some(i) {
                    st = st.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
                }
                TCell::from(crate::tui::clip(&t, widths[i])).style(st)
            }));
            if r == row {
                line.style(theme::selected())
            } else {
                line
            }
        });
    let w: Vec<Constraint> = shown
        .iter()
        .map(|&i| Constraint::Length(u16::try_from(widths[i]).unwrap_or(32)))
        .collect();
    f.render_widget(Table::new(body, w).header(header).column_spacing(1), area);
}

/// Whether a key in the cell dialog means "set NULL" (Ctrl-N).
#[must_use]
pub fn is_null_key(k: &KeyEvent) -> bool {
    k.code == KeyCode::Char('n') && k.modifiers.contains(KeyModifiers::CONTROL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view() -> TableView {
        let mut v = TableView::new("shop", "orders", false);
        v.cols = col_infos(
            &json!({"columns": {"orders": [
                {"name": "id", "type": "int(11)", "key": "PRI", "nullable": false},
                {"name": "status", "type": "varchar(80)", "key": ""},
                {"name": "total", "type": "decimal(10,2)", "key": ""},
                {"name": "twice", "type": "int(11)", "key": "", "generated": true}
            ]}}),
            "orders",
        );
        v.set_page(ResultSet {
            columns: vec![
                Column::named("id"),
                Column::named("status"),
                Column {
                    type_name: "DECIMAL".into(),
                    ..Column::named("total")
                },
                Column::named("twice"),
            ],
            rows: vec![
                vec![
                    Some("1".into()),
                    Some("open".into()),
                    Some("10.50".into()),
                    Some("2".into()),
                ],
                vec![
                    Some("2".into()),
                    None,
                    Some("20.00".into()),
                    Some("4".into()),
                ],
            ],
            ..ResultSet::default()
        });
        v
    }

    #[test]
    fn pages_follow_the_key_inside_the_filter() {
        let mut v = view();
        assert_eq!(
            v.page_sql(),
            "SELECT * FROM `shop`.`orders` ORDER BY `id` LIMIT 101"
        );
        v.filter = "status = 'open'".into();
        v.starts.push(Some(vec![Some("2".into())]));
        assert_eq!(
            v.page_sql(),
            "SELECT * FROM `shop`.`orders` WHERE (status = 'open') AND `id` > '2' ORDER BY `id` LIMIT 101"
        );
        v.cols.clear();
        assert_eq!(
            v.page_sql(),
            "SELECT * FROM `shop`.`orders` WHERE (status = 'open') LIMIT 101"
        );
    }

    #[test]
    fn staged_changes_become_guarded_statements() {
        let mut v = view();
        v.stage(0, 1, Some("paid".into()));
        v.stage(0, 3, Some("9".into())); // generated: never written
        v.stage(1, 1, None); // the value read: unstaged
        assert_eq!(v.pending(), 1);
        v.deleted.insert(1);
        v.data_key(KeyEvent::from(KeyCode::Char('o')));
        v.stage(2, 0, Some("3".into()));
        v.stage(2, 1, Some("it's".into()));
        assert_eq!(v.pending(), 3);
        let s: Vec<String> = v.statements().into_iter().map(|s| s.sql).collect();
        assert_eq!(
            s,
            [
                "UPDATE `shop`.`orders` SET `status` = 'paid' WHERE `id` <=> '1' AND `status` <=> 'open' \
                 AND CAST(`total` AS CHAR) <=> '10.50' AND `twice` <=> '2' LIMIT 1",
                "DELETE FROM `shop`.`orders` WHERE `id` <=> '2' AND `status` <=> NULL \
                 AND CAST(`total` AS CHAR) <=> '20.00' AND `twice` <=> '4' LIMIT 1",
                "INSERT INTO `shop`.`orders` (`id`, `status`) VALUES ('3', 'it''s')",
            ]
        );
        assert_eq!(
            v.statements()[0].reread.as_deref(),
            Some("SELECT * FROM `shop`.`orders` WHERE `id` <=> '1'")
        );
        match v.data_key(KeyEvent::from(KeyCode::Char('y'))) {
            TableCmd::Copy(text, 3) => assert!(text.ends_with("VALUES ('3', 'it''s');")),
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(v.data_key(KeyEvent::from(KeyCode::Char('a'))), TableCmd::Apply(s) if s.len() == 3)
        );
        v.data_key(KeyEvent::from(KeyCode::Char('r')));
        assert_eq!(v.pending(), 0);
    }

    #[test]
    fn conflicts_take_the_current_values_and_keep_edits() {
        let mut v = view();
        v.stage(0, 2, Some("11.00".into()));
        v.deleted.insert(1);
        v.conflicts = vec![
            (
                0,
                Some(vec![
                    Some("1".into()),
                    Some("shipped".into()),
                    Some("10.50".into()),
                    Some("2".into()),
                ]),
            ),
            (1, None),
        ];
        assert!(v.conflict_note().contains("row 1 is now (1, shipped"));
        v.data_key(KeyEvent::from(KeyCode::Char('t')));
        assert!(v.deleted.is_empty());
        let s = v.statements();
        assert_eq!(s.len(), 1);
        assert!(
            s[0].sql.contains("SET `total` = '11.00'")
                && s[0].sql.contains("`status` <=> 'shipped'")
        );
    }

    #[test]
    fn filters_paging_and_read_only_views() {
        let mut v = view();
        v.col = 1;
        assert_eq!(
            v.data_key(KeyEvent::from(KeyCode::Char('='))),
            TableCmd::Query(
                Purpose::Page,
                "SELECT * FROM `shop`.`orders` WHERE (`status` = 'open') ORDER BY `id` LIMIT 101"
                    .into()
            )
        );
        v.has_more = true;
        v.rows = vec![vec![Some("7".into())]];
        assert!(matches!(
            v.data_key(KeyEvent::from(KeyCode::Char('n'))),
            TableCmd::Query(..)
        ));
        assert_eq!(v.starts.last(), Some(&Some(vec![Some("7".into())])));
        assert!(matches!(
            v.data_key(KeyEvent::from(KeyCode::Char('p'))),
            TableCmd::Query(..)
        ));
        assert_eq!(v.starts, vec![None]);

        let mut w = TableView::new("shop", "open_orders", true);
        assert_eq!(w.subs(), &VIEW_SUBS);
        assert!(
            matches!(w.data_key(KeyEvent::from(KeyCode::Char('e'))), TableCmd::Status(s) if s.contains("read-only"))
        );
        assert_eq!(
            w.key(KeyEvent::from(KeyCode::Char(']'))),
            TableCmd::None // Columns: from the catalog
        );
        assert_eq!(
            w.key(KeyEvent::from(KeyCode::Char(']'))),
            TableCmd::Query(
                Purpose::Structure(Sub::Policies),
                "SHOW POLICIES FROM `shop`".into()
            )
        );
    }

    #[test]
    fn structure_tabs_are_shaped() {
        let v = view();
        let ddl = v.shape(
            Sub::Ddl,
            vec![ResultSet {
                columns: vec![Column::named("Table"), Column::named("Create Table")],
                rows: vec![vec![
                    Some("orders".into()),
                    Some("CREATE TABLE `orders` (\n  `id` int\n)".into()),
                ]],
                ..ResultSet::default()
            }],
        );
        assert_eq!(ddl.rows.len(), 3);
        let pol = v.shape(
            Sub::Policies,
            vec![ResultSet {
                columns: vec![Column::named("Policy"), Column::named("Table")],
                rows: vec![
                    vec![Some("p1".into()), Some("orders".into())],
                    vec![Some("p2".into()), Some("parts".into())],
                ],
                ..ResultSet::default()
            }],
        );
        assert_eq!(pol.rows.len(), 1);
        let rel = v.shape(
            Sub::Relations,
            vec![
                ResultSet {
                    columns: vec![Column::named("direction")],
                    rows: vec![vec![Some("out".into())]],
                    ..ResultSet::default()
                },
                ResultSet::default(),
                ResultSet {
                    columns: vec![Column::named("direction")],
                    rows: vec![vec![Some("in".into())]],
                    ..ResultSet::default()
                },
            ],
        );
        assert_eq!(rel.rows.len(), 2);
        // The shape `/v1/shards` returns.
        let sh = v.shards_set(&json!({"groups": [{"name": "g1", "kind": "sharded", "state": "active",
            "key_spec": {"parts": [{"class": "numeric"}]},
            "map": {"shards": [1, 2]}, "members": [
                {"db": "shop", "table": "customers", "routing": "columns", "key_columns": ["customer_id"]},
                {"db": "shop", "table": "orders", "routing": "parent", "key_columns": []}]}]}));
        assert_eq!(
            sh.rows[1],
            vec![Some("Key".into()), Some("customer_id (numeric)".into())]
        );
        assert_eq!(
            sh.rows[2],
            vec![Some("Routing".into()), Some("parent".into())]
        );
        assert_eq!(sh.rows[4], vec![Some("Shards".into()), Some("2".into())]);
        assert_eq!(
            v.shards_set(&json!({"groups": []})).rows[0][0].as_deref(),
            Some("Sharded")
        );
        assert_eq!(v.columns_set().rows.len(), 4);
    }
}
