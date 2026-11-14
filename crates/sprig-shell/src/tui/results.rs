//! Results views: a grid, a JSON tree and a graph
//! path view, over the results of the last run in a tab.

use crate::tui::theme;
use std::collections::BTreeSet;
use std::fmt::Write as _;

use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell as TCell, Paragraph, Row, Table};
use serde_json::Value;
use unicode_width::UnicodeWidthStr;

use crate::error::ShellError;
use crate::model::{ResultEvent, ResultSet};
use crate::output::cell_text;

/// How a result is shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum View {
    /// Rows and columns.
    #[default]
    Grid,
    /// Documents or rows as folding JSON.
    Json,
    /// Nodes, relationships and paths as text.
    Graph,
}

impl View {
    /// The next view (`F3`).
    #[must_use]
    pub fn next(self) -> Self {
        match self {
            View::Grid => View::Json,
            View::Json => View::Graph,
            View::Graph => View::Grid,
        }
    }

    fn name(self) -> &'static str {
        match self {
            View::Grid => "grid",
            View::Json => "json",
            View::Graph => "graph",
        }
    }
}

/// What a tab shows below the editor.
#[derive(Debug, Clone, Default)]
pub struct Results {
    /// Finished and streaming results of the last run.
    pub sets: Vec<ResultSet>,
    events: Vec<ResultEvent>,
    /// Which result is shown.
    pub current: usize,
    /// The view.
    pub view: View,
    /// Selected row.
    pub row: usize,
    /// First visible row.
    pub top: usize,
    /// First visible column.
    pub left: usize,
    /// Folded JSON paths.
    pub folded: BTreeSet<String>,
    /// The last error.
    pub error: Option<ShellError>,
    /// A one-line note: "3 rows · 12 ms", "cancelled".
    pub note: String,
    /// The statement being run, redacted, while it runs.
    pub running: Option<String>,
}

const MAX_COL: usize = 40;

impl Results {
    /// Clears for a new run.
    pub fn start(&mut self, stmt: String) {
        *self = Self {
            view: self.view,
            running: Some(stmt),
            ..Self::default()
        };
    }

    /// Adds streamed events.
    pub fn push(&mut self, evs: Vec<ResultEvent>) {
        self.events.extend(evs);
        self.sets = ResultSet::from_events(&self.events);
        if self.current >= self.sets.len() {
            self.current = self.sets.len().saturating_sub(1);
        }
    }

    /// The shown result.
    #[must_use]
    pub fn set(&self) -> Option<&ResultSet> {
        self.sets.get(self.current)
    }

    /// The selected cell as text, for the detail popup.
    #[must_use]
    pub fn selected_cell(&self) -> Option<String> {
        let s = self.set()?;
        if let Some(d) = s.docs.get(self.row) {
            return Some(serde_json::to_string_pretty(d).unwrap_or_default());
        }
        let r = s.rows.get(self.row)?;
        let i = self.left.min(s.columns.len().saturating_sub(1));
        let c = r.get(i)?.clone();
        Some(c.map_or_else(|| "NULL".to_owned(), |v| pretty_if_json(&v)))
    }

    /// Moves the selection by `d` rows.
    pub fn move_rows(&mut self, d: isize) {
        if self.view != View::Grid {
            // Lines of the JSON and graph views are counted when drawn,
            // which clamps the row.
            self.row = self.row.saturating_add_signed(d).min(1 << 24);
            return;
        }
        let n = self.set().map_or(0, |s| s.rows.len().max(s.docs.len()));
        if n == 0 {
            return;
        }
        let r = self.row.saturating_add_signed(d).min(n - 1);
        self.row = r;
    }

    /// Moves the first visible column.
    pub fn move_cols(&mut self, d: isize) {
        let n = self.set().map_or(0, |s| s.columns.len());
        self.left = self.left.saturating_add_signed(d).min(n.saturating_sub(1));
    }

    /// Shows the next or previous result of a multi-result run.
    pub fn cycle_result(&mut self, forward: bool) {
        if self.sets.is_empty() {
            return;
        }
        self.current = if forward {
            (self.current + 1) % self.sets.len()
        } else {
            (self.current + self.sets.len() - 1) % self.sets.len()
        };
        self.row = 0;
        self.top = 0;
        self.left = 0;
    }

    /// The title line of the pane.
    #[must_use]
    pub fn title(&self) -> String {
        let mut t = format!(" Results [{}]", self.view.name());
        if self.sets.len() > 1 {
            let _ = write!(t, " {}/{}", self.current + 1, self.sets.len());
        }
        if let Some(s) = &self.running {
            let _ = write!(t, " · running {}", crate::tui::clip(s, 40));
        } else if !self.note.is_empty() {
            let _ = write!(t, " · {}", self.note);
        }
        t.push(' ');
        t
    }

    /// Draws the pane.
    pub fn draw(&mut self, f: &mut Frame<'_>, area: Rect, block: Block<'_>) {
        let inner = block.inner(area);
        f.render_widget(block, area);
        if let Some(e) = &self.error {
            let p = Paragraph::new(Line::styled(e.to_string(), theme::error()))
                .wrap(ratatui::widgets::Wrap { trim: false });
            f.render_widget(p, inner);
            return;
        }
        let Some(set) = self.sets.get(self.current).cloned() else {
            if self.running.is_some() {
                f.render_widget(Paragraph::new("waiting for the first rows..."), inner);
            } else if self.note.is_empty() {
                welcome(f, inner);
            }
            return;
        };
        match self.view {
            View::Grid if !set.columns.is_empty() => self.draw_grid(f, inner, &set),
            View::Graph => {
                let lines = graph_lines(&set);
                let lines = if lines.is_empty() {
                    vec![Line::from(
                        "no nodes, relationships or paths in this result",
                    )]
                } else {
                    lines
                };
                self.draw_lines(f, inner, lines);
            }
            _ => {
                let lines = self.json_lines(&set);
                self.draw_lines(f, inner, lines);
            }
        }
    }

    fn draw_lines(&mut self, f: &mut Frame<'_>, area: Rect, lines: Vec<Line<'static>>) {
        let h = area.height as usize;
        self.row = self.row.min(lines.len().saturating_sub(1));
        if self.row < self.top {
            self.top = self.row;
        } else if h > 0 && self.row >= self.top + h {
            self.top = self.row + 1 - h;
        }
        let shown: Vec<Line<'static>> = lines
            .into_iter()
            .enumerate()
            .skip(self.top)
            .take(h)
            .map(|(i, l)| {
                if i == self.row {
                    l.style(theme::selected())
                } else {
                    l
                }
            })
            .collect();
        f.render_widget(Paragraph::new(shown), area);
    }

    #[allow(clippy::many_single_char_names)]
    fn draw_grid(&mut self, f: &mut Frame<'_>, area: Rect, set: &ResultSet) {
        let cols: Vec<usize> = (self.left..set.columns.len()).collect();
        let mut widths: Vec<usize> = cols
            .iter()
            .map(|&i| {
                let c = &set.columns[i];
                let mut w = c.name.width().max(c.type_name.width()).max(3);
                for r in set.rows.iter().take(200) {
                    let t = cell_text(&r.get(i).cloned().flatten(), c);
                    w = w.max(t.width());
                }
                w.min(MAX_COL)
            })
            .collect();
        // Fit what the width allows.
        let mut used = 0;
        let mut keep = 0;
        for w in &widths {
            if used + w + 1 > area.width as usize && keep > 0 {
                break;
            }
            used += w + 1;
            keep += 1;
        }
        widths.truncate(keep);
        let header = Row::new(cols.iter().take(keep).map(|&i| {
            let c = &set.columns[i];
            TCell::from(vec![
                Line::styled(c.name.clone(), Style::new().add_modifier(Modifier::BOLD)),
                Line::styled(c.type_name.to_ascii_lowercase(), theme::muted()),
            ])
        }))
        .height(2);
        let h = area.height.saturating_sub(2) as usize;
        self.row = self.row.min(set.rows.len().saturating_sub(1));
        if self.row < self.top {
            self.top = self.row;
        } else if h > 0 && self.row >= self.top + h {
            self.top = self.row + 1 - h;
        }
        let rows = set
            .rows
            .iter()
            .enumerate()
            .skip(self.top)
            .take(h)
            .map(|(ri, r)| {
                let cells = cols.iter().take(keep).enumerate().map(|(k, &i)| {
                    let c = &set.columns[i];
                    let v = r.get(i).cloned().flatten();
                    let mut t = cell_text(&v, c);
                    if t.width() > widths[k] {
                        t = crate::tui::clip(&t, widths[k]);
                    }
                    let t = if c.numeric() {
                        format!("{t:>w$}", w = widths[k])
                    } else {
                        t
                    };
                    let style = if v.is_none() {
                        theme::muted()
                    } else {
                        Style::new()
                    };
                    TCell::from(Span::styled(t, style))
                });
                let row = Row::new(cells);
                if ri == self.row {
                    row.style(theme::selected())
                } else {
                    row
                }
            });
        let constraints: Vec<Constraint> = widths
            .iter()
            .map(|w| Constraint::Length(u16::try_from(*w).unwrap_or(u16::MAX)))
            .collect();
        f.render_widget(
            Table::new(rows, constraints)
                .header(header)
                .column_spacing(1),
            area,
        );
    }

    fn json_lines(&self, set: &ResultSet) -> Vec<Line<'static>> {
        let docs: Vec<Value> = if set.docs.is_empty() {
            set.rows
                .iter()
                .map(|r| {
                    let mut m = serde_json::Map::new();
                    for (i, c) in set.columns.iter().enumerate() {
                        let v = r.get(i).cloned().flatten();
                        let v = v.map_or(Value::Null, |s| {
                            if c.type_name.eq_ignore_ascii_case("json") {
                                serde_json::from_str(&s).unwrap_or(Value::String(s))
                            } else {
                                crate::output::cell_json(&Some(s), c)
                            }
                        });
                        m.insert(c.name.clone(), v);
                    }
                    Value::Object(m)
                })
                .collect()
        } else {
            set.docs.clone()
        };
        let mut out = Vec::new();
        for (i, d) in docs.iter().enumerate() {
            tree(&mut out, &format!("{i}"), None, d, 0, &self.folded);
        }
        if docs.is_empty()
            && let Some(e) = &set.end
        {
            out.push(Line::from(format!(
                "{} rows affected{}",
                e.affected,
                if e.info.is_empty() {
                    String::new()
                } else {
                    format!(" · {}", e.info)
                }
            )));
        }
        out
    }

    /// Folds or unfolds the JSON node on the selected line.
    pub fn toggle_fold(&mut self) {
        let Some(set) = self.set().cloned() else {
            return;
        };
        let mut paths = Vec::new();
        let docs: Vec<Value> = if set.docs.is_empty() {
            Vec::new()
        } else {
            set.docs.clone()
        };
        for (i, d) in docs.iter().enumerate() {
            tree_paths(&mut paths, &format!("{i}"), d, &self.folded);
        }
        if let Some(Some(p)) = paths.get(self.row)
            && !self.folded.remove(p)
        {
            self.folded.insert(p.clone());
        }
    }
}

fn pretty_if_json(s: &str) -> String {
    let t = s.trim_start();
    if (t.starts_with('{') || t.starts_with('['))
        && let Ok(v) = serde_json::from_str::<Value>(s)
    {
        return serde_json::to_string_pretty(&v).unwrap_or_else(|_| s.to_owned());
    }
    s.to_owned()
}

/// Extended JSON's typed values shown with a badge.
fn badge(v: &Value) -> Option<(String, String)> {
    let o = v.as_object()?;
    if o.len() != 1 {
        return None;
    }
    let (k, inner) = o.iter().next()?;
    let name = match k.as_str() {
        "$oid" => "ObjectId",
        "$date" => "Date",
        "$numberLong" => "Int64",
        "$numberInt" => "Int32",
        "$numberDecimal" => "Decimal128",
        "$numberDouble" => "Double",
        "$binary" => "Binary",
        "$timestamp" => "Timestamp",
        _ => return None,
    };
    let text = match inner {
        Value::String(s) => s.clone(),
        Value::Object(m) => m
            .get("$numberLong")
            .and_then(Value::as_str)
            .map_or_else(|| inner.to_string(), str::to_owned),
        other => other.to_string(),
    };
    Some((name.to_owned(), text))
}

fn scalar(v: &Value) -> Span<'static> {
    match v {
        Value::Null => Span::styled("null", theme::muted()),
        Value::Bool(b) => Span::styled(b.to_string(), theme::number()),
        Value::Number(n) => Span::styled(n.to_string(), theme::number()),
        Value::String(s) => Span::styled(format!("{s:?}"), theme::string()),
        other => Span::raw(other.to_string()),
    }
}

fn tree(
    out: &mut Vec<Line<'static>>,
    path: &str,
    key: Option<&str>,
    v: &Value,
    depth: usize,
    folded: &BTreeSet<String>,
) {
    let indent = "  ".repeat(depth);
    let key_span = key.map(|k| Span::styled(format!("{k}: "), theme::keyword()));
    let mut line = vec![Span::raw(indent.clone())];
    if let Some((name, text)) = badge(v) {
        line.push(Span::raw("  "));
        line.extend(key_span);
        line.push(Span::styled(format!("[{name}] "), theme::muted()));
        line.push(Span::styled(text, theme::string()));
        out.push(Line::from(line));
        return;
    }
    match v {
        Value::Object(m) if !m.is_empty() => {
            let open = !folded.contains(path);
            line.push(Span::raw(if open { "▾ " } else { "▸ " }));
            line.extend(key_span);
            line.push(Span::raw(if open {
                "{".to_owned()
            } else {
                format!(
                    "{{…{} key{}}}",
                    m.len(),
                    if m.len() == 1 { "" } else { "s" }
                )
            }));
            out.push(Line::from(line));
            if open {
                for (k, c) in m {
                    tree(out, &format!("{path}.{k}"), Some(k), c, depth + 1, folded);
                }
                out.push(Line::from(format!("{indent}  }}")));
            }
        }
        Value::Array(a) if !a.is_empty() => {
            let open = !folded.contains(path);
            line.push(Span::raw(if open { "▾ " } else { "▸ " }));
            line.extend(key_span);
            line.push(Span::raw(if open {
                "[".to_owned()
            } else {
                format!("[…{} item{}]", a.len(), if a.len() == 1 { "" } else { "s" })
            }));
            out.push(Line::from(line));
            if open {
                for (i, c) in a.iter().enumerate() {
                    tree(out, &format!("{path}[{i}]"), None, c, depth + 1, folded);
                }
                out.push(Line::from(format!("{indent}  ]")));
            }
        }
        _ => {
            line.push(Span::raw("  "));
            line.extend(key_span);
            line.push(scalar(v));
            out.push(Line::from(line));
        }
    }
}

/// The fold path of each line `tree` draws (`None` for closing lines and
/// scalars).
fn tree_paths(out: &mut Vec<Option<String>>, path: &str, v: &Value, folded: &BTreeSet<String>) {
    if badge(v).is_some() {
        out.push(None);
        return;
    }
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push(Some(path.to_owned()));
            if !folded.contains(path) {
                for (k, c) in m {
                    tree_paths(out, &format!("{path}.{k}"), c, folded);
                }
                out.push(None);
            }
        }
        Value::Array(a) if !a.is_empty() => {
            out.push(Some(path.to_owned()));
            if !folded.contains(path) {
                for (i, c) in a.iter().enumerate() {
                    tree_paths(out, &format!("{path}[{i}]"), c, folded);
                }
                out.push(None);
            }
        }
        _ => out.push(None),
    }
}

fn props(v: Option<&Value>) -> String {
    match v.and_then(Value::as_object) {
        Some(m) if !m.is_empty() => {
            let parts: Vec<String> = m.iter().map(|(k, v)| format!("{k}: {v}")).collect();
            format!(" {{{}}}", parts.join(", "))
        }
        _ => String::new(),
    }
}

fn node_text(n: &Value) -> String {
    let labels: String = n
        .get("labels")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .fold(String::new(), |mut acc, l| {
                    let _ = write!(acc, ":{l}");
                    acc
                })
        })
        .unwrap_or_default();
    let id = n.get("id").map(Value::to_string).unwrap_or_default();
    format!("({id}{labels}{})", props(n.get("props")))
}

fn rel_text(r: &Value, forward: bool) -> String {
    let t = r.get("type").and_then(Value::as_str).unwrap_or_default();
    let body = format!("[:{t}{}]", props(r.get("props")));
    if forward {
        format!("-{body}->")
    } else {
        format!("<-{body}-")
    }
}

/// Graph values found in a result: `{"$node"}`, `{"$rel"}`, `{"$path"}`,
/// as the Bolt client produces them, in documents and JSON cells.
fn collect_graph(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m)
            if m.contains_key("$node") || m.contains_key("$rel") || m.contains_key("$path") =>
        {
            out.push(v.clone());
        }
        Value::Object(m) => m.values().for_each(|c| collect_graph(c, out)),
        Value::Array(a) => a.iter().for_each(|c| collect_graph(c, out)),
        _ => {}
    }
}

/// The graph view: one line per path, then each node with its
/// relationships (an adjacency list).
#[must_use]
pub fn graph_lines(set: &ResultSet) -> Vec<Line<'static>> {
    let mut found = Vec::new();
    for d in &set.docs {
        collect_graph(d, &mut found);
    }
    for r in &set.rows {
        for c in r.iter().flatten() {
            if let Ok(v) = serde_json::from_str::<Value>(c) {
                collect_graph(&v, &mut found);
            }
        }
    }
    let mut lines = Vec::new();
    let mut nodes: Vec<Value> = Vec::new();
    let mut rels: Vec<Value> = Vec::new();
    for v in &found {
        if let Some(p) = v.get("$path").and_then(Value::as_array) {
            let mut s = String::new();
            let mut last_id = None;
            for el in p {
                if let Some(n) = el.get("$node") {
                    s.push_str(&node_text(n));
                    last_id = n.get("id").cloned();
                    nodes.push(n.clone());
                } else if let Some(r) = el.get("$rel") {
                    let fwd = r.get("start") == last_id.as_ref() || last_id.is_none();
                    s.push_str(&rel_text(r, fwd));
                    rels.push(r.clone());
                }
            }
            lines.push(Line::from(s));
        } else if let Some(n) = v.get("$node") {
            nodes.push(n.clone());
        } else if let Some(r) = v.get("$rel") {
            rels.push(r.clone());
        }
    }
    let mut seen = BTreeSet::new();
    nodes.retain(|n| seen.insert(n.get("id").map(Value::to_string).unwrap_or_default()));
    if !lines.is_empty() && !nodes.is_empty() {
        lines.push(Line::from(""));
    }
    for n in &nodes {
        lines.push(Line::styled(
            node_text(n),
            Style::new().add_modifier(Modifier::BOLD),
        ));
        let id = n.get("id");
        for r in &rels {
            if r.get("start") == id {
                let end = r.get("end").map(Value::to_string).unwrap_or_default();
                lines.push(Line::from(format!("  {}({end})", rel_text(r, true))));
            } else if r.get("end") == id {
                let start = r.get("start").map(Value::to_string).unwrap_or_default();
                lines.push(Line::from(format!("  {}({start})", rel_text(r, false))));
            }
        }
    }
    if nodes.is_empty() && lines.is_empty() {
        for r in &rels {
            let s = r.get("start").map(Value::to_string).unwrap_or_default();
            let e = r.get("end").map(Value::to_string).unwrap_or_default();
            lines.push(Line::from(format!("({s}){}({e})", rel_text(r, true))));
        }
    }
    lines
}

/// The empty results pane before anything has run: the mascot waving and
/// the keys to start with. Nothing when the pane is too small.
fn welcome(f: &mut Frame<'_>, area: Rect) {
    use crate::tui::mascot::{self, ColourMode};
    let mode = match theme::depth() {
        theme::Depth::None => ColourMode::None,
        theme::Depth::Ansi256 => ColourMode::Ansi256,
        theme::Depth::TrueColor => ColourMode::TrueColor,
    };
    let art = &mascot::BUDDY;
    let tips = [
        ("Ctrl-K", "search tables, tabs and commands"),
        ("F5", "run the statement at the cursor"),
        ("F1", "all the keys"),
    ];
    let w = art.width() + 4 + 42;
    let h = art.height().max(u16::try_from(tips.len()).unwrap_or(3) + 2);
    if area.width < w || area.height < h {
        return;
    }
    let left = area.x + (area.width - w) / 2;
    let top = area.y + (area.height - h) / 2;
    f.render_widget(
        Paragraph::new(mascot::render(art, mode)),
        Rect {
            x: left,
            y: top,
            width: art.width(),
            height: art.height(),
        },
    );
    let mut lines = vec![
        Line::styled("Ready when you are.", theme::accent()),
        Line::from(""),
    ];
    for (k, what) in tips {
        lines.push(Line::from(vec![
            Span::styled(format!("{k:<8}"), theme::keyword()),
            Span::styled(what, theme::muted()),
        ]));
    }
    f.render_widget(
        Paragraph::new(lines),
        Rect {
            x: left + art.width() + 4,
            y: top + 1,
            width: 42,
            height: h.saturating_sub(1),
        },
    );
}
