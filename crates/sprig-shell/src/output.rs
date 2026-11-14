//! Output formats of the CLI: table, JSON,
//! NDJSON, CSV and TSV, written as results stream in.
//!
//! - **table** buffers one result to size its columns, as `mysql` does;
//! - **json** writes each result as one array of row objects (documents as
//!   they are), streamed row by row;
//! - **ndjson** writes one object per row or document;
//! - **csv** (RFC 4180; NULL is an empty field, an empty string `""`) and
//!   **tsv** (`\t`, `\n` and `\\` escaped; NULL is `\N`, as `LOAD DATA`
//!   reads it) write a header line per result and a blank line between
//!   results.
//!
//! A statement without rows prints `Query OK, n rows affected` in table
//! form and `{"affected_rows": n, ...}` in JSON forms; CSV and TSV print
//! nothing for it.

use std::fmt::Write as _;
use std::io::Write;

use base64::Engine as _;
use serde_json::{Map, Value, json};
use unicode_width::UnicodeWidthStr;

use crate::model::{Cell, Column, EndInfo, ResultEvent, Sink};

/// An output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Boxed table.
    Table,
    /// JSON arrays.
    Json,
    /// One JSON object per line.
    Ndjson,
    /// Comma-separated.
    Csv,
    /// Tab-separated.
    Tsv,
}

impl Format {
    /// Parses a format name.
    ///
    /// # Errors
    /// The accepted names, for anything else.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "table" => Ok(Format::Table),
            "json" => Ok(Format::Json),
            "ndjson" | "jsonl" => Ok(Format::Ndjson),
            "csv" => Ok(Format::Csv),
            "tsv" | "tab" => Ok(Format::Tsv),
            _ => Err(format!("format {s:?}: use table, json, ndjson, csv or tsv")),
        }
    }
}

/// Writes results in one format.
pub struct Writer<W: Write + Send> {
    out: W,
    format: Format,
    columns: Vec<Column>,
    rows: Vec<Vec<Cell>>,
    docs: Vec<Value>,
    in_result: bool,
    first_row: bool,
    results: usize,
}

/// A cell as JSON: integers as numbers, other values as strings.
#[must_use]
pub fn cell_json(c: &Cell, col: &Column) -> Value {
    match c {
        None => Value::Null,
        Some(s) if col.numeric() && !s.contains(['.', 'e', 'E']) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<u64>().map(Value::from))
            .unwrap_or_else(|_| Value::String(s.clone())),
        Some(s) => Value::String(s.clone()),
    }
}

/// A cell for the table: binary as `0x` hex, control characters escaped.
#[must_use]
pub fn cell_text(c: &Cell, col: &Column) -> String {
    match c {
        None => "NULL".to_owned(),
        Some(s) if col.binary => match base64::engine::general_purpose::STANDARD.decode(s) {
            Ok(b) => {
                let mut h = String::with_capacity(2 + b.len() * 2);
                h.push_str("0x");
                for x in b {
                    let _ = write!(h, "{x:02X}");
                }
                h
            }
            Err(_) => s.clone(),
        },
        Some(s) => s
            .chars()
            .flat_map(|ch| match ch {
                '\n' => vec!['\\', 'n'],
                '\r' => vec!['\\', 'r'],
                '\t' => vec!['\\', 't'],
                c if c.is_control() => vec!['?'],
                c => vec![c],
            })
            .collect(),
    }
}

fn csv_field(s: &str) -> String {
    if s.is_empty() || s.contains([',', '"', '\n', '\r']) || s.starts_with(' ') || s.ends_with(' ')
    {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

fn tsv_field(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => o.push_str("\\\\"),
            '\t' => o.push_str("\\t"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o
}

fn end_json(e: &EndInfo) -> Value {
    let mut m = Map::new();
    m.insert("affected_rows".into(), json!(e.affected));
    if e.last_insert_id != 0 {
        m.insert("last_insert_id".into(), json!(e.last_insert_id));
    }
    if e.warnings != 0 {
        m.insert("warnings".into(), json!(e.warnings));
    }
    if !e.info.is_empty() {
        m.insert("info".into(), json!(e.info));
    }
    Value::Object(m)
}

impl<W: Write + Send> Writer<W> {
    /// A writer of `format` to `out`.
    pub fn new(out: W, format: Format) -> Self {
        Self {
            out,
            format,
            columns: Vec::new(),
            rows: Vec::new(),
            docs: Vec::new(),
            in_result: false,
            first_row: true,
            results: 0,
        }
    }

    /// The underlying writer.
    pub fn into_inner(self) -> W {
        self.out
    }

    /// Flushes the output.
    ///
    /// # Errors
    /// I/O errors.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }

    fn row_object(&self, row: &[Cell]) -> Value {
        let mut m = Map::new();
        for (i, c) in self.columns.iter().enumerate() {
            let v = row.get(i).cloned().flatten();
            let mut key = c.name.clone();
            let mut n = 2;
            while m.contains_key(&key) {
                key = format!("{}_{n}", c.name);
                n += 1;
            }
            m.insert(key, cell_json(&v, c));
        }
        Value::Object(m)
    }

    fn header(&mut self, cols: Vec<Column>) -> std::io::Result<()> {
        self.columns = cols;
        self.rows.clear();
        self.in_result = true;
        self.first_row = true;
        match self.format {
            Format::Json => {
                self.out.write_all(b"[")?;
            }
            Format::Csv | Format::Tsv => {
                if self.results > 0 {
                    writeln!(self.out)?;
                }
                let sep = if self.format == Format::Csv {
                    ","
                } else {
                    "\t"
                };
                let line: Vec<String> = self
                    .columns
                    .iter()
                    .map(|c| {
                        if self.format == Format::Csv {
                            csv_field(&c.name)
                        } else {
                            tsv_field(&c.name)
                        }
                    })
                    .collect();
                writeln!(self.out, "{}", line.join(sep))?;
            }
            Format::Table | Format::Ndjson => {}
        }
        Ok(())
    }

    fn rows(&mut self, rows: Vec<Vec<Cell>>) -> std::io::Result<()> {
        match self.format {
            Format::Table => self.rows.extend(rows),
            Format::Json => {
                for r in rows {
                    let sep = if self.first_row { "\n  " } else { ",\n  " };
                    self.first_row = false;
                    let o = self.row_object(&r);
                    write!(self.out, "{sep}{o}")?;
                }
            }
            Format::Ndjson => {
                for r in rows {
                    let o = self.row_object(&r);
                    writeln!(self.out, "{o}")?;
                }
            }
            Format::Csv | Format::Tsv => {
                for r in rows {
                    let fields: Vec<String> = r
                        .iter()
                        .map(|c| match (c, self.format) {
                            (None, Format::Csv) => String::new(),
                            (None, _) => "\\N".to_owned(),
                            (Some(s), Format::Csv) => csv_field(s),
                            (Some(s), _) => tsv_field(s),
                        })
                        .collect();
                    let sep = if self.format == Format::Csv {
                        ","
                    } else {
                        "\t"
                    };
                    writeln!(self.out, "{}", fields.join(sep))?;
                }
            }
        }
        Ok(())
    }

    fn doc(&mut self, d: Value) -> std::io::Result<()> {
        match self.format {
            Format::Ndjson | Format::Csv | Format::Tsv => writeln!(self.out, "{d}"),
            Format::Json => {
                if !self.in_result {
                    self.in_result = true;
                    self.first_row = true;
                    self.out.write_all(b"[")?;
                }
                let sep = if self.first_row { "\n  " } else { ",\n  " };
                self.first_row = false;
                write!(self.out, "{sep}{d}")
            }
            Format::Table => {
                self.docs.push(d);
                Ok(())
            }
        }
    }

    fn end(&mut self, e: &EndInfo) -> std::io::Result<()> {
        let had_columns = !self.columns.is_empty();
        let had_docs = !self.docs.is_empty()
            || (self.format == Format::Json && self.in_result && !had_columns);
        match self.format {
            Format::Table => {
                if had_columns {
                    self.write_table()?;
                    let n = self.rows.len();
                    let s = if n == 1 { "row" } else { "rows" };
                    if e.truncated {
                        writeln!(self.out, "first {n} {s}; the rest were left out at the cap")?;
                    } else {
                        writeln!(self.out, "{n} {s}")?;
                    }
                } else if !self.docs.is_empty() {
                    for d in std::mem::take(&mut self.docs) {
                        writeln!(
                            self.out,
                            "{}",
                            serde_json::to_string_pretty(&d).unwrap_or_default()
                        )?;
                    }
                } else {
                    let s = if e.affected == 1 { "row" } else { "rows" };
                    write!(self.out, "Query OK, {} {s} affected", e.affected)?;
                    if e.last_insert_id != 0 {
                        write!(self.out, ", last insert id {}", e.last_insert_id)?;
                    }
                    writeln!(self.out)?;
                    if !e.info.is_empty() {
                        writeln!(self.out, "{}", e.info)?;
                    }
                }
                if e.warnings > 0 {
                    writeln!(self.out, "{} warning(s)", e.warnings)?;
                }
            }
            Format::Json => {
                if self.in_result {
                    let close = if self.first_row { "]" } else { "\n]" };
                    writeln!(self.out, "{close}")?;
                } else {
                    writeln!(self.out, "{}", end_json(e))?;
                }
            }
            Format::Ndjson => {
                if !had_columns && !had_docs {
                    writeln!(self.out, "{}", end_json(e))?;
                }
            }
            Format::Csv | Format::Tsv => {}
        }
        if e.truncated && self.format != Format::Table {
            // Tell scripts on stderr; stdout stays parsable.
            eprintln!("sprig: the result was cut at the row or byte cap");
        }
        self.columns.clear();
        self.rows.clear();
        self.docs.clear();
        self.in_result = false;
        self.results += 1;
        Ok(())
    }

    fn write_table(&mut self) -> std::io::Result<()> {
        let cols = &self.columns;
        let texts: Vec<Vec<String>> = self
            .rows
            .iter()
            .map(|r| {
                cols.iter()
                    .enumerate()
                    .map(|(i, c)| cell_text(&r.get(i).cloned().flatten(), c))
                    .collect()
            })
            .collect();
        let mut w: Vec<usize> = cols.iter().map(|c| c.name.width()).collect();
        for r in &texts {
            for (i, t) in r.iter().enumerate() {
                w[i] = w[i].max(t.width());
            }
        }
        let rule: String = w.iter().fold(String::from("+"), |mut s, n| {
            s.push_str(&"-".repeat(n + 2));
            s.push('+');
            s
        });
        let pad = |t: &str, n: usize, right: bool| {
            let fill = " ".repeat(n.saturating_sub(t.width()));
            if right {
                format!("{fill}{t}")
            } else {
                format!("{t}{fill}")
            }
        };
        writeln!(self.out, "{rule}")?;
        let head: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| pad(&c.name, w[i], false))
            .collect();
        writeln!(self.out, "| {} |", head.join(" | "))?;
        writeln!(self.out, "{rule}")?;
        for r in &texts {
            let line: Vec<String> = r
                .iter()
                .enumerate()
                .map(|(i, t)| pad(t, w[i], cols[i].numeric()))
                .collect();
            writeln!(self.out, "| {} |", line.join(" | "))?;
        }
        if !texts.is_empty() {
            writeln!(self.out, "{rule}")?;
        }
        Ok(())
    }

    /// Ends output: closes a result the stream left open (an error mid-way).
    ///
    /// # Errors
    /// I/O errors.
    pub fn finish(&mut self) -> std::io::Result<()> {
        if self.in_result || !self.columns.is_empty() || !self.docs.is_empty() {
            let e = EndInfo::default();
            if self.format == Format::Table && self.columns.is_empty() && self.docs.is_empty() {
                self.in_result = false;
            } else {
                self.end(&e)?;
            }
        }
        self.out.flush()
    }
}

impl<W: Write + Send> Sink for Writer<W> {
    fn event(&mut self, ev: ResultEvent) -> std::io::Result<()> {
        match ev {
            ResultEvent::Header(c) => self.header(c),
            ResultEvent::Rows(r) => self.rows(r),
            ResultEvent::Doc(d) => self.doc(d),
            ResultEvent::End(e) => self.end(&e),
        }
    }
}

#[cfg(test)]
mod tests;
