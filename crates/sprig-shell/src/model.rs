//! Results as the shell sees them, whatever the transport (chapter 53,
//! "Crate layout": `ResultEvent`).

use serde_json::Value;

/// One column of a result.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Column {
    /// The name the client sees.
    pub name: String,
    /// The type's name (`BIGINT`, `VARCHAR`, ...), or empty.
    pub type_name: String,
    /// Cells are base64 of bytes that aren't text.
    pub binary: bool,
}

impl Column {
    /// A column with a name only.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    /// Whether the type is numeric (right-aligned in tables).
    #[must_use]
    pub fn numeric(&self) -> bool {
        let t = self.type_name.to_ascii_uppercase();
        let t = t.split([' ', '(']).next().unwrap_or_default();
        matches!(
            t,
            "TINYINT"
                | "SMALLINT"
                | "MEDIUMINT"
                | "INT"
                | "INTEGER"
                | "BIGINT"
                | "DECIMAL"
                | "NEWDECIMAL"
                | "FLOAT"
                | "DOUBLE"
                | "LONG"
                | "LONGLONG"
                | "SHORT"
                | "TINY"
                | "INT24"
                | "YEAR"
        )
    }
}

/// A cell: text, or `None` for SQL `NULL`.
pub type Cell = Option<String>;

/// The end of one result.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndInfo {
    /// Rows changed.
    pub affected: u64,
    /// `LAST_INSERT_ID()`, 0 for none.
    pub last_insert_id: u64,
    /// Warnings.
    pub warnings: u32,
    /// The server's info text.
    pub info: String,
    /// Rows were left out at a cap.
    pub truncated: bool,
}

/// One event of a statement's results.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultEvent {
    /// A new tabular result and its columns.
    Header(Vec<Column>),
    /// Rows of the current result.
    Rows(Vec<Vec<Cell>>),
    /// A document, a Redis reply or a graph record.
    Doc(Value),
    /// The end of the current result.
    End(EndInfo),
}

/// Where results go.
pub trait Sink: Send {
    /// One event.
    ///
    /// # Errors
    /// When the output can't be written (a closed pipe).
    fn event(&mut self, ev: ResultEvent) -> std::io::Result<()>;
}

/// Keeps every event, for tests and the TUI's buffers.
#[derive(Debug, Default)]
pub struct Collect(pub Vec<ResultEvent>);

impl Sink for Collect {
    fn event(&mut self, ev: ResultEvent) -> std::io::Result<()> {
        self.0.push(ev);
        Ok(())
    }
}

/// A finished tabular or document result, as the TUI keeps it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResultSet {
    /// Columns; empty for documents or statements without rows.
    pub columns: Vec<Column>,
    /// Rows.
    pub rows: Vec<Vec<Cell>>,
    /// Documents (MongoDB, Redis replies, graph records).
    pub docs: Vec<Value>,
    /// The end, once it came.
    pub end: Option<EndInfo>,
}

impl ResultSet {
    /// Splits a list of events into results.
    #[must_use]
    pub fn from_events(events: &[ResultEvent]) -> Vec<ResultSet> {
        let mut out: Vec<ResultSet> = Vec::new();
        let mut cur: Option<ResultSet> = None;
        for ev in events {
            match ev {
                ResultEvent::Header(cols) => {
                    if let Some(c) = cur.take() {
                        out.push(c);
                    }
                    cur = Some(ResultSet {
                        columns: cols.clone(),
                        ..ResultSet::default()
                    });
                }
                ResultEvent::Rows(r) => cur
                    .get_or_insert_with(ResultSet::default)
                    .rows
                    .extend(r.iter().cloned()),
                ResultEvent::Doc(d) => cur
                    .get_or_insert_with(ResultSet::default)
                    .docs
                    .push(d.clone()),
                ResultEvent::End(e) => {
                    let mut c = cur.take().unwrap_or_default();
                    c.end = Some(e.clone());
                    out.push(c);
                }
            }
        }
        if let Some(c) = cur {
            out.push(c);
        }
        out
    }
}
