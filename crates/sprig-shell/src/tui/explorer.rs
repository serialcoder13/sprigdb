//! The explorer tree (chapter 53, "The TUI"): databases, then their
//! tables, views, routines, streams and sinks, from `Catalog.ListObjects`,
//! `Catalog.Snapshot`, `SHOW STREAMS FROM` and `SHOW SINKS FROM`, as the
//! user may see them. Databases load when first opened.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use serde_json::Value;

/// What a node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A database.
    Database,
    /// A heading: "tables", "views", ...
    Group,
    /// A table (also collections and key namespaces, which are tables).
    Table,
    /// A view.
    View,
    /// A function or procedure.
    Routine,
    /// An event stream.
    Stream,
    /// A sink.
    Sink,
    /// A column of a table.
    Column,
    /// A message ("loading...", an error).
    Note,
}

/// One node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The text shown.
    pub label: String,
    /// Extra text shown dimmed (a column's type, a row count).
    pub detail: String,
    /// The kind.
    pub kind: Kind,
    /// The database it is in.
    pub db: String,
    /// Children.
    pub children: Vec<Node>,
    /// Open.
    pub open: bool,
    /// Children were loaded (databases).
    pub loaded: bool,
}

impl Node {
    fn new(kind: Kind, db: &str, label: &str) -> Self {
        Self {
            label: label.to_owned(),
            detail: String::new(),
            kind,
            db: db.to_owned(),
            children: Vec::new(),
            open: false,
            loaded: false,
        }
    }
}

/// The tree and its selection.
#[derive(Debug, Clone, Default)]
pub struct Explorer {
    /// Top-level nodes (databases).
    pub roots: Vec<Node>,
    /// Selected visible row.
    pub selected: usize,
    /// First visible row.
    pub top: usize,
    /// A message instead of the tree ("loading", an error).
    pub note: Option<String>,
}

/// What the app should do after an explorer key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplorerAction {
    /// Nothing more.
    None,
    /// Load a database's objects.
    Load(String),
    /// Put this statement in the editor and run it.
    Run(String),
    /// Insert this text at the editor's cursor.
    Insert(String),
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

impl Explorer {
    /// Sets the databases from `Catalog.ListObjects`, keeping what was
    /// loaded and open.
    pub fn set_databases(&mut self, v: &Value) {
        let names: Vec<String> = v
            .get("databases")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let old = std::mem::take(&mut self.roots);
        self.roots = names
            .iter()
            .map(|n| {
                old.iter()
                    .find(|o| &o.label == n)
                    .cloned()
                    .unwrap_or_else(|| Node::new(Kind::Database, n, n))
            })
            .collect();
        self.note = None;
    }

    /// Fills a database from its snapshot and its streams and sinks
    /// (each a `SHOW ...` result as rows of names; empty when not allowed).
    pub fn set_database(
        &mut self,
        db: &str,
        snapshot: &Value,
        streams: &[String],
        sinks: &[String],
    ) {
        let Some(node) = self.roots.iter_mut().find(|n| n.label == db) else {
            return;
        };
        let cols = snapshot.get("columns").and_then(Value::as_object);
        let mut groups: Vec<(Kind, &str, Vec<Node>)> = vec![
            (Kind::Table, "tables", Vec::new()),
            (Kind::View, "views", Vec::new()),
            (Kind::Routine, "routines", Vec::new()),
            (Kind::Stream, "streams", Vec::new()),
            (Kind::Sink, "sinks", Vec::new()),
        ];
        for o in snapshot
            .get("objects")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let name = o.get("name").and_then(Value::as_str).unwrap_or_default();
            let kind = match o.get("kind").and_then(Value::as_str) {
                Some("view") => Kind::View,
                Some("function" | "procedure") => Kind::Routine,
                _ => Kind::Table,
            };
            let mut n = Node::new(kind, db, name);
            if let Some(r) = o.get("rows").and_then(Value::as_u64) {
                n.detail = format!("~{r} rows");
            }
            for c in cols
                .and_then(|m| m.get(name))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let mut cn = Node::new(
                    Kind::Column,
                    db,
                    c.get("name").and_then(Value::as_str).unwrap_or_default(),
                );
                cn.detail = String::from(c.get("type").and_then(Value::as_str).unwrap_or_default());
                n.children.push(cn);
            }
            if let Some(g) = groups.iter_mut().find(|g| g.0 == kind) {
                g.2.push(n);
            }
        }
        for (list, kind) in [(streams, Kind::Stream), (sinks, Kind::Sink)] {
            if let Some(g) = groups.iter_mut().find(|g| g.0 == kind) {
                g.2.extend(list.iter().map(|s| Node::new(kind, db, s)));
            }
        }
        node.children = groups
            .into_iter()
            .filter(|g| !g.2.is_empty())
            .map(|(_, label, children)| {
                let mut g = Node::new(Kind::Group, db, label);
                g.detail = children.len().to_string();
                g.children = children;
                g.open = label == "tables";
                g
            })
            .collect();
        if node.children.is_empty() {
            node.children.push(Node::new(Kind::Note, db, "(empty)"));
        }
        node.loaded = true;
        node.open = true;
    }

    /// Marks a database as failed to load.
    pub fn set_error(&mut self, db: &str, msg: &str) {
        if let Some(node) = self.roots.iter_mut().find(|n| n.label == db) {
            node.children = vec![Node::new(Kind::Note, db, msg)];
            node.loaded = true;
            node.open = true;
        }
    }

    /// The visible rows: depth and a path of indexes.
    fn visible(&self) -> Vec<(usize, Vec<usize>)> {
        fn walk(
            nodes: &[Node],
            depth: usize,
            path: &mut Vec<usize>,
            out: &mut Vec<(usize, Vec<usize>)>,
        ) {
            for (i, n) in nodes.iter().enumerate() {
                path.push(i);
                out.push((depth, path.clone()));
                if n.open {
                    walk(&n.children, depth + 1, path, out);
                }
                path.pop();
            }
        }
        let mut out = Vec::new();
        walk(&self.roots, 0, &mut Vec::new(), &mut out);
        out
    }

    fn node_mut(&mut self, path: &[usize]) -> Option<&mut Node> {
        let (first, rest) = path.split_first()?;
        let mut n = self.roots.get_mut(*first)?;
        for &i in rest {
            n = n.children.get_mut(i)?;
        }
        Some(n)
    }

    fn node(&self, path: &[usize]) -> Option<&Node> {
        let (first, rest) = path.split_first()?;
        let mut n = self.roots.get(*first)?;
        for &i in rest {
            n = n.children.get(i)?;
        }
        Some(n)
    }

    /// The selected node.
    #[must_use]
    pub fn selected_node(&self) -> Option<&Node> {
        let v = self.visible();
        v.get(self.selected).and_then(|(_, p)| self.node(p))
    }

    /// Moves the selection.
    pub fn move_by(&mut self, d: isize) {
        let n = self.visible().len();
        if n > 0 {
            self.selected = self.selected.saturating_add_signed(d).min(n - 1);
        }
    }

    /// Opens or closes the selected node; `Enter` on an object browses it.
    pub fn activate(&mut self, enter: bool) -> ExplorerAction {
        let v = self.visible();
        let Some((_, path)) = v.get(self.selected).cloned() else {
            return ExplorerAction::None;
        };
        let Some(n) = self.node_mut(&path) else {
            return ExplorerAction::None;
        };
        let (db, name) = (n.db.clone(), n.label.clone());
        match n.kind {
            Kind::Database if !n.loaded => {
                n.open = true;
                n.children = vec![Node::new(Kind::Note, &db, "loading...")];
                ExplorerAction::Load(db)
            }
            Kind::Table | Kind::View if enter => {
                ExplorerAction::Run(format!("SELECT * FROM {}.{} LIMIT 100;", q(&db), q(&name)))
            }
            Kind::Database | Kind::Group | Kind::Table | Kind::View => {
                n.open = !n.open;
                ExplorerAction::None
            }
            Kind::Stream => ExplorerAction::Run(format!(
                "SELECT partition_no, offset_no, ts, `key`, `value` FROM {}.{} ORDER BY ts DESC LIMIT 100;",
                q(&db),
                q(&name)
            )),
            Kind::Sink => ExplorerAction::Run(format!("SHOW SINK STATUS {}.{};", q(&db), q(&name))),
            Kind::Routine => ExplorerAction::Insert(format!("CALL {}.{}()", q(&db), q(&name))),
            Kind::Column => ExplorerAction::Insert(q(&name)),
            Kind::Note => ExplorerAction::None,
        }
    }

    /// Closes the selected node, or moves to its parent.
    pub fn close(&mut self) {
        let v = self.visible();
        let Some((_, path)) = v.get(self.selected).cloned() else {
            return;
        };
        if let Some(n) = self.node_mut(&path)
            && n.open
        {
            n.open = false;
            return;
        }
        if path.len() > 1 {
            let parent = &path[..path.len() - 1];
            if let Some(i) = v.iter().position(|(_, p)| p == parent) {
                self.selected = i;
            }
        }
    }

    /// The lines to draw in `height` rows.
    #[must_use]
    pub fn lines(&mut self, height: usize, focused: bool) -> Vec<Line<'static>> {
        if let Some(n) = &self.note {
            return vec![Line::styled(n.clone(), Style::new().fg(Color::DarkGray))];
        }
        let v = self.visible();
        if self.selected < self.top {
            self.top = self.selected;
        } else if height > 0 && self.selected >= self.top + height {
            self.top = self.selected + 1 - height;
        }
        v.iter()
            .enumerate()
            .skip(self.top)
            .take(height)
            .filter_map(|(i, (depth, path))| {
                let n = self.node(path)?;
                let marker = match n.kind {
                    Kind::Database | Kind::Group | Kind::Table | Kind::View
                        if n.open || !n.loaded && n.kind == Kind::Database =>
                    {
                        if n.open {
                            "▾ "
                        } else {
                            "▸ "
                        }
                    }
                    Kind::Database | Kind::Group => "▸ ",
                    Kind::Table | Kind::View if !n.children.is_empty() => "▸ ",
                    _ => "  ",
                };
                let color = match n.kind {
                    Kind::Database => Color::Yellow,
                    Kind::Group => Color::Blue,
                    Kind::Stream | Kind::Sink => Color::Magenta,
                    Kind::Note => Color::DarkGray,
                    _ => Color::Reset,
                };
                let mut style = Style::new().fg(color);
                if i == self.selected && focused {
                    style = style.add_modifier(Modifier::REVERSED);
                }
                let mut spans = vec![
                    Span::raw("  ".repeat(*depth)),
                    Span::styled(format!("{marker}{}", n.label), style),
                ];
                if !n.detail.is_empty() {
                    spans.push(Span::styled(
                        format!(" {}", n.detail),
                        Style::new().fg(Color::DarkGray),
                    ));
                }
                Some(Line::from(spans))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tree_and_actions() {
        let mut e = Explorer::default();
        e.set_databases(&json!({"databases": ["shop", "hr"]}));
        assert_eq!(e.activate(true), ExplorerAction::Load("shop".into()));
        e.set_database(
            "shop",
            &json!({"objects": [
                {"name": "orders", "kind": "table", "rows": 12},
                {"name": "v_paid", "kind": "view"},
                {"name": "f", "kind": "function"}],
              "columns": {"orders": [{"name": "id", "type": "bigint"}]}}),
            &["order_events".to_owned()],
            &[],
        );
        // shop, tables, orders, views, routines, streams, hr
        e.move_by(2);
        assert_eq!(e.selected_node().map(|n| n.label.as_str()), Some("orders"));
        assert_eq!(
            e.activate(true),
            ExplorerAction::Run("SELECT * FROM `shop`.`orders` LIMIT 100;".into())
        );
        e.move_by(3);
        assert_eq!(e.selected_node().map(|n| n.kind), Some(Kind::Group));
        e.close();
        e.move_by(-10);
        e.close();
        assert_eq!(e.lines(10, true).len(), 2);
    }
}
