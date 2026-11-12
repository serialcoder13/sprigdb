//! The TUI's state, keys and drawing (chapter 53, "The TUI").
//!
//! [`App`] is pure: keys change state and return [`Effect`]s, which the
//! runtime in [`crate::tui`] carries out, sending [`Msg`]s back. That keeps
//! every screen testable with ratatui's `TestBackend`.

use crate::tui::icons::{self, Icon};
use crate::tui::palette::{self, Action as PalAction, Item as PalItem};
use crate::tui::theme;
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph, Row, Table, Tabs, Wrap};
use serde_json::Value;

use crate::admin::Captured;
use crate::dash::{Panel, VIEW_NAMES};
use crate::error::ShellError;
use crate::history::History;
use crate::inbox::{Inbox, InboxAction};
use crate::lang::{Class, Lang, classify_sql, destructive, split};
use crate::model::{ResultEvent, ResultSet};
use crate::output::Format;
use crate::profile::Confirm;
use crate::redact::redact;
use crate::saved::Saved;
use crate::tui::complete::{Catalog, complete};
use crate::tui::editor::Editor;
use crate::tui::explorer::{self, Explorer, ExplorerAction};
use crate::tui::results::{Results, View};
use crate::tui::table::{
    ApplyStmt, Conflict, Purpose, TableCmd, TableView, col_infos, is_null_key,
};

/// A data tab's id.
pub type TabId = u64;

/// One data tab: a connection, an editor and its results.
#[derive(Debug)]
pub struct DataTab {
    /// Stable id.
    pub id: TabId,
    /// The language typed in the editor.
    pub lang: Lang,
    /// The editor.
    pub editor: Editor,
    /// The last run's results.
    pub results: Results,
    /// Whether a statement runs.
    pub running: bool,
    /// The connection, for the status bar.
    pub describe: String,
    /// The current database, for completion.
    pub database: Option<String>,
    /// A transaction is open.
    pub in_txn: bool,
    /// When the running statement started (for the status bar).
    pub started: Option<std::time::Instant>,
    /// The running statements, redacted, for the catalog's history.
    pub last_run: Option<(String, Lang)>,
    /// A table tab's view (instead of the editor and results).
    pub table: Option<Box<TableView>>,
}

impl DataTab {
    /// A new tab.
    #[must_use]
    pub fn new(id: TabId, lang: Lang, describe: String, database: Option<String>) -> Self {
        Self {
            id,
            lang,
            editor: Editor::default(),
            results: Results::default(),
            running: false,
            describe,
            database,
            in_txn: false,
            started: None,
            last_run: None,
            table: None,
        }
    }
}

/// Which pane has the keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    /// The explorer tree.
    Explorer,
    /// The editor.
    Editor,
    /// The results.
    Results,
}

/// The fixed screens after the data tabs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fixed {
    /// Live dashboards.
    Dash,
    /// `sprigctl` commands.
    Admin,
    /// Approvals.
    Inbox,
}

const FIXED: [Fixed; 3] = [Fixed::Dash, Fixed::Admin, Fixed::Inbox];

/// What waits for a yes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pending {
    /// Statements to run in a tab.
    Run(TabId, Vec<String>, Lang),
    /// A `sprigctl` command (with `--yes` added).
    Admin(Vec<String>),
    /// `KILL [QUERY] id`.
    Kill(String, bool),
    /// An inbox action.
    Inbox(InboxAction),
    /// A table tab's staged changes, in its database.
    TableApply(TabId, String, Vec<ApplyStmt>),
}

/// A popup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dialog {
    /// A yes/no question.
    Confirm(String, Pending),
    /// The keys.
    Help,
    /// A cell's full value.
    Cell(String),
    /// Choose an export format.
    Export,
    /// A line of text for an inbox action (days, a window, a comment).
    Input(String, String, InboxAction),
    /// The command palette: the query typed and the selected row.
    Palette(String, usize),
    /// A name for the editor's text (Ctrl-S).
    SaveAs(String),
    /// The table view's filter, or a cell's new value.
    TableInput(TableField, String),
}

/// What [`Dialog::TableInput`] asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableField {
    /// The WHERE clause.
    Filter,
    /// The selected cell's value.
    Cell,
}

/// The completion menu.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    /// What the word becomes.
    pub items: Vec<String>,
    /// The highlighted item.
    pub selected: usize,
    /// The typed word's length in characters.
    pub word_len: usize,
}

/// A finished run, for the catalog's history (`Library.Record`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    /// The language.
    pub lang: Lang,
    /// The database it ran in.
    pub database: Option<String>,
    /// The statements, redacted.
    pub text: String,
    /// Whether it succeeded.
    pub ok: bool,
    /// Milliseconds.
    pub ms: u64,
    /// The first error.
    pub error: Option<String>,
}

impl RunRecord {
    /// The request body.
    #[must_use]
    pub fn body(&self) -> Value {
        serde_json::json!({
            "language": self.lang.name(),
            "database": self.database,
            "text": self.text,
            "ok": self.ok,
            "ms": self.ms,
            "error": self.error,
            "client": "shell",
        })
    }
}

/// Something for the runtime to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Run statements in a tab, in order, stopping at an error.
    Run(TabId, Vec<String>, Lang),
    /// Cancel a tab's statement.
    Cancel(TabId),
    /// Open a new data tab like the current one.
    NewTab(Lang),
    /// Close a tab's connection.
    CloseTab(TabId),
    /// Read the database list.
    LoadCatalog,
    /// Read one database's objects.
    LoadDb(String),
    /// Poll a dashboard view now.
    DashRefresh(String),
    /// `KILL [QUERY] id`.
    Kill(String, bool),
    /// Run `sprigctl` with these arguments.
    Admin(Vec<String>),
    /// Write the shown result to a file.
    Export(TabId, Format),
    /// Refresh the inbox.
    InboxRefresh,
    /// Act on a recommendation.
    InboxAct(InboxAction),
    /// Read the saved queries and history the tenant's catalog keeps.
    LoadLibrary,
    /// Send a saved query to the catalog.
    LibrarySave(crate::saved::SavedQuery),
    /// Record a run in the catalog's history.
    LibraryRecord(RunRecord),
    /// Read a table tab's database snapshot (its columns and key).
    TableInfo(TabId, String),
    /// Run a table tab's query in its database.
    TableQuery(TabId, String, Purpose, String),
    /// Read `/v1/shards` for a table tab.
    TableShards(TabId),
    /// Apply a table tab's statements in one transaction in its database.
    TableApply(TabId, String, Vec<ApplyStmt>),
    /// Put text on the terminal's clipboard (OSC 52).
    Copy(String),
    /// Quit.
    Quit,
}

/// Something that happened in the runtime.
#[derive(Debug)]
pub enum Msg {
    /// Streamed results.
    Events(TabId, Vec<ResultEvent>),
    /// A run ended.
    Done {
        /// The tab.
        tab: TabId,
        /// The first error.
        error: Option<ShellError>,
        /// Milliseconds.
        elapsed_ms: u128,
        /// The connection afterwards.
        describe: String,
        /// The database afterwards.
        database: Option<String>,
        /// A transaction is open afterwards.
        in_txn: bool,
    },
    /// A new tab connected.
    TabOpened(Box<DataTab>),
    /// The database list.
    Catalog(Result<Value, ShellError>),
    /// A database's snapshot, streams and sinks.
    Db(String, Result<Value, ShellError>, Vec<String>, Vec<String>),
    /// A dashboard poll.
    Dash(Panel),
    /// A `sprigctl` run's output.
    Admin(Vec<String>, Result<Captured, ShellError>),
    /// The inbox.
    Inbox(Result<Value, ShellError>),
    /// The catalog's library (`GET /v1/library`).
    Library(Result<Value, ShellError>),
    /// A table tab's database snapshot.
    TableInfo(TabId, Result<Value, ShellError>),
    /// A table tab's query results.
    TableRows(TabId, Purpose, Result<Vec<ResultSet>, ShellError>),
    /// `/v1/shards` for a table tab.
    TableShards(TabId, Result<Value, ShellError>),
    /// The apply's conflicts (none: committed).
    TableApplied(TabId, Result<Vec<Conflict>, ShellError>),
    /// A line for the status bar.
    Status(String),
}

/// The Dashboard screen's state.
#[derive(Debug, Default)]
pub struct DashState {
    /// The view shown.
    pub view: usize,
    /// The last poll of each view.
    pub panels: Vec<Option<Panel>>,
    /// Selected row.
    pub row: usize,
    /// When the shown view was last polled.
    pub polled: Option<std::time::Instant>,
}

/// The Admin screen's state.
#[derive(Debug, Default)]
pub struct AdminState {
    /// The command line.
    pub input: Editor,
    /// Output lines, newest last.
    pub output: Vec<Line<'static>>,
    /// Lines scrolled up from the bottom.
    pub scroll: usize,
    /// Commands run, for Up and Down.
    pub history: Vec<String>,
    /// Position in `history`.
    pub pos: usize,
    /// A command runs.
    pub running: bool,
}

/// `sprigctl` commands that change or remove things the TUI confirms
/// before adding `--yes` (sprigctl asks on a terminal, and the Admin tab
/// gives it none).
const ADMIN_DESTRUCTIVE: &[&[&str]] = &[
    &["tenant", "delete"],
    &["tenant", "failover"],
    &["tenant", "move"],
    &["cluster", "retire"],
    &["cluster", "drain"],
    &["dc", "failover"],
    &["job", "cancel"],
    &["job", "cutover"],
    &["shard", "move"],
    &["index", "drop"],
    &["sink", "drop"],
    &["sink", "delete"],
    &["txn", "resolve"],
    &["settings", "reset"],
];

/// Whether a `sprigctl` command is one the TUI confirms.
#[must_use]
pub fn admin_destructive(args: &[String]) -> bool {
    let words: Vec<&str> = args
        .iter()
        .filter(|a| !a.starts_with('-'))
        .map(String::as_str)
        .collect();
    ADMIN_DESTRUCTIVE
        .iter()
        .any(|d| words.len() >= d.len() && words[..d.len()] == **d)
}

/// The whole TUI.
#[derive(Debug)]
pub struct App {
    /// Data tabs.
    pub tabs: Vec<DataTab>,
    /// The screen: an index into the data tabs, then the fixed screens.
    pub screen: usize,
    /// Which pane has the keys on a data tab.
    pub focus: Focus,
    /// The explorer.
    pub explorer: Explorer,
    /// What completion knows.
    pub catalog: Catalog,
    /// History of the profile.
    pub history: History,
    /// Saved queries of the profile (in memory until the runtime loads them).
    pub saved: Saved,
    /// A popup.
    pub dialog: Option<Dialog>,
    /// The completion menu.
    pub completion: Option<Completion>,
    /// The status line.
    pub status: String,
    /// When to ask before running.
    pub confirm: Confirm,
    /// The profile's name.
    pub profile: String,
    /// Dashboards.
    pub dash: DashState,
    /// The Admin tab.
    pub admin: AdminState,
    /// The approvals inbox.
    pub inbox: Inbox,
    /// The user can use operator views (an admin profile).
    pub operator: bool,
    /// Next tab id.
    pub next_id: TabId,
    /// Quit after this frame.
    pub quit: bool,
}

fn ctrl(k: &KeyEvent) -> bool {
    k.modifiers.contains(KeyModifiers::CONTROL)
}

fn alt(k: &KeyEvent) -> bool {
    k.modifiers.contains(KeyModifiers::ALT)
}

impl App {
    /// An app with one data tab.
    #[must_use]
    pub fn new(
        profile: &str,
        first: DataTab,
        history: History,
        confirm: Confirm,
        operator: bool,
    ) -> Self {
        Self {
            next_id: first.id + 1,
            tabs: vec![first],
            screen: 0,
            focus: Focus::Editor,
            explorer: Explorer {
                note: Some("loading...".to_owned()),
                ..Explorer::default()
            },
            catalog: Catalog::default(),
            history,
            saved: Saved::default(),
            dialog: None,
            completion: None,
            status: "F1 help · F5 run · Ctrl-C cancel · Ctrl-Q quit".to_owned(),
            confirm,
            profile: profile.to_owned(),
            dash: DashState {
                panels: vec![None; VIEW_NAMES.len()],
                ..DashState::default()
            },
            admin: AdminState::default(),
            inbox: Inbox::default(),
            operator,
            quit: false,
        }
    }

    /// The fixed screen shown, if any.
    #[must_use]
    pub fn fixed(&self) -> Option<Fixed> {
        self.screen
            .checked_sub(self.tabs.len())
            .and_then(|i| FIXED.get(i).copied())
    }

    /// Shows a fixed screen.
    pub fn show(&mut self, f: Fixed) -> Vec<Effect> {
        let i = FIXED.iter().position(|x| *x == f).unwrap_or(0);
        self.screen = self.tabs.len() + i;
        self.entered()
    }

    fn entered(&mut self) -> Vec<Effect> {
        match self.fixed() {
            Some(Fixed::Dash) => vec![Effect::DashRefresh(VIEW_NAMES[self.dash.view].to_owned())],
            Some(Fixed::Inbox) => vec![Effect::InboxRefresh],
            _ => Vec::new(),
        }
    }

    fn tab_mut(&mut self) -> Option<&mut DataTab> {
        if self.screen < self.tabs.len() {
            self.tabs.get_mut(self.screen)
        } else {
            None
        }
    }

    /// The tab with `id`.
    pub fn tab_by_id(&mut self, id: TabId) -> Option<&mut DataTab> {
        self.tabs.iter_mut().find(|t| t.id == id)
    }

    /// Handles one key.
    pub fn on_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        if k.kind == KeyEventKind::Release {
            return Vec::new();
        }
        if let Some(d) = self.dialog.clone() {
            return self.dialog_key(&d, k);
        }
        // Global keys.
        match k.code {
            KeyCode::Char('q') if ctrl(&k) => {
                self.quit = true;
                return vec![Effect::Quit];
            }
            KeyCode::F(1) => {
                self.dialog = Some(Dialog::Help);
                return Vec::new();
            }
            KeyCode::Char('k' | 'p') if ctrl(&k) => {
                self.dialog = Some(Dialog::Palette(String::new(), 0));
                self.completion = None;
                return Vec::new();
            }
            KeyCode::F(6) => return self.switch(-1),
            KeyCode::F(7) => return self.switch(1),
            KeyCode::Char(c @ '1'..='9') if alt(&k) => {
                let i = (c as usize) - ('1' as usize);
                if i < self.tabs.len() + FIXED.len() {
                    self.screen = i;
                    self.completion = None;
                    return self.entered();
                }
                return Vec::new();
            }
            KeyCode::Char('t') if ctrl(&k) => {
                let lang = self.tabs.get(self.screen).map_or(Lang::Sql, |t| t.lang);
                self.status = String::from("opening a tab...");
                return vec![Effect::NewTab(lang)];
            }
            KeyCode::Char('w')
                if ctrl(&k) && self.screen < self.tabs.len() && self.tabs.len() > 1 =>
            {
                let t = self.tabs.remove(self.screen);
                self.screen = self.screen.min(self.tabs.len() - 1);
                return vec![Effect::CloseTab(t.id)];
            }
            _ => {}
        }
        match self.fixed() {
            Some(Fixed::Dash) => self.dash_key(k),
            Some(Fixed::Admin) => self.admin_key(k),
            Some(Fixed::Inbox) => self.inbox_key(k),
            None => self.data_key(k),
        }
    }

    fn switch(&mut self, d: isize) -> Vec<Effect> {
        let n = self.tabs.len() + FIXED.len();
        self.screen = (self.screen + n).saturating_add_signed(d) % n;
        self.completion = None;
        self.entered()
    }

    fn dialog_key(&mut self, d: &Dialog, k: KeyEvent) -> Vec<Effect> {
        match d {
            Dialog::Confirm(_, pending) => match k.code {
                KeyCode::Char('y' | 'Y') => {
                    self.dialog = None;
                    let p = pending.clone();
                    match p {
                        Pending::Run(tab, stmts, lang) => self.start_run(tab, stmts, lang),
                        Pending::Admin(mut args) => {
                            args.push("--yes".to_owned());
                            self.admin.running = true;
                            vec![Effect::Admin(args)]
                        }
                        Pending::Kill(id, q) => vec![Effect::Kill(id, q)],
                        Pending::Inbox(a) => vec![Effect::InboxAct(a)],
                        Pending::TableApply(id, db, stmts) => {
                            self.status = format!("applying {} change(s)...", stmts.len());
                            vec![Effect::TableApply(id, db, stmts)]
                        }
                    }
                }
                KeyCode::Char('n' | 'N') | KeyCode::Esc => {
                    self.dialog = None;
                    self.status = String::from("not run");
                    Vec::new()
                }
                _ => Vec::new(),
            },
            Dialog::Export => {
                let f = match k.code {
                    KeyCode::Char('c') => Some(Format::Csv),
                    KeyCode::Char('j') => Some(Format::Json),
                    KeyCode::Char('n') => Some(Format::Ndjson),
                    KeyCode::Char('t') => Some(Format::Tsv),
                    _ => None,
                };
                self.dialog = None;
                match (f, self.tabs.get(self.screen)) {
                    (Some(f), Some(t)) => vec![Effect::Export(t.id, f)],
                    _ => Vec::new(),
                }
            }
            Dialog::Input(title, text, action) => {
                let mut text = text.clone();
                match k.code {
                    KeyCode::Enter => {
                        self.dialog = None;
                        return vec![Effect::InboxAct(action.clone().with_input(&text))];
                    }
                    KeyCode::Esc => {
                        self.dialog = None;
                        return Vec::new();
                    }
                    KeyCode::Backspace => {
                        text.pop();
                    }
                    KeyCode::Char(c) => text.push(c),
                    _ => {}
                }
                self.dialog = Some(Dialog::Input(title.clone(), text, action.clone()));
                Vec::new()
            }
            Dialog::Help | Dialog::Cell(_) => {
                self.dialog = None;
                Vec::new()
            }
            Dialog::Palette(q, sel) => self.palette_key(q, *sel, k),
            Dialog::TableInput(field, text) => self.table_input_key(*field, text, k),
            Dialog::SaveAs(name) => {
                let mut name = name.clone();
                match k.code {
                    KeyCode::Esc => {
                        self.dialog = None;
                        return Vec::new();
                    }
                    KeyCode::Enter => {
                        self.dialog = None;
                        return self.save_as(&name);
                    }
                    KeyCode::Backspace => {
                        name.pop();
                    }
                    KeyCode::Char(c) if !ctrl(&k) => name.push(c),
                    _ => {}
                }
                self.dialog = Some(Dialog::SaveAs(name));
                Vec::new()
            }
        }
    }

    /// Saves the current tab's text as `name` (Ctrl-S), here and, when it
    /// keeps them, in the tenant's catalog.
    fn save_as(&mut self, name: &str) -> Vec<Effect> {
        let Some(t) = self.tabs.get(self.screen) else {
            return Vec::new();
        };
        let (text, lang, db) = (t.editor.text(), t.lang, t.database.clone());
        match self.saved.save(name, &text, lang, db.as_deref()) {
            Ok(Some(q)) => {
                self.status = format!("saved as \"{}\"", q.name);
                if self.saved.shared {
                    vec![Effect::LibrarySave(q)]
                } else {
                    Vec::new()
                }
            }
            Ok(None) => {
                self.status = String::from("nothing to save");
                Vec::new()
            }
            Err(e) => {
                self.status = format!("not saved: {e}");
                Vec::new()
            }
        }
    }

    /// The palette's history: this machine's runs, then the catalog's
    /// (Studio's, other machines') that aren't among them.
    fn history_items(&self, out: &mut Vec<PalItem>) {
        let mut seen = std::collections::HashSet::new();
        let local = self
            .history
            .entries
            .iter()
            .rev()
            .take(50)
            .map(|e| (e.lang.as_str(), e.text.as_str(), ""));
        let shared = self
            .saved
            .shared_history
            .iter()
            .take(100)
            .map(|h| (h.lang.as_str(), h.text.as_str(), h.client.as_str()));
        for (lang, text, client) in local.chain(shared) {
            if !seen.insert(text) {
                continue;
            }
            out.push(PalItem {
                label: format!(
                    "{} {}",
                    icons::glyph(Icon::History),
                    crate::tui::clip(text.lines().next().unwrap_or(""), 60)
                ),
                hint: if client.is_empty() || client == "shell" {
                    format!("history · {lang}")
                } else {
                    format!("history · {lang} · {client}")
                },
                action: PalAction::Insert(text.to_owned()),
            });
        }
    }

    /// Everything the palette searches: open screens, loaded tables, commands.
    #[must_use]
    pub fn palette_items(&self) -> Vec<PalItem> {
        let mut out = Vec::new();
        for (i, t) in self.tabs.iter().enumerate() {
            out.push(PalItem {
                label: match &t.table {
                    Some(v) => format!(
                        "{} {} {}.{}",
                        i + 1,
                        icons::glyph(Icon::Table),
                        v.db,
                        v.table
                    ),
                    None => format!(
                        "{} {} {}",
                        i + 1,
                        t.lang.name(),
                        t.database.as_deref().unwrap_or("-")
                    ),
                },
                hint: "open tab".to_owned(),
                action: PalAction::Screen(i),
            });
        }
        for (j, (name, hint)) in [
            ("Dashboard", "screen"),
            ("Admin", "screen · sprigctl"),
            ("Inbox", "screen · approvals"),
        ]
        .into_iter()
        .enumerate()
        {
            out.push(PalItem {
                label: name.to_owned(),
                hint: hint.to_owned(),
                action: PalAction::Screen(self.tabs.len() + j),
            });
        }
        for db in &self.explorer.roots {
            for group in &db.children {
                for n in &group.children {
                    if matches!(n.kind, explorer::Kind::Table | explorer::Kind::View) {
                        out.push(PalItem {
                            label: n.label.clone(),
                            hint: format!(
                                "{} · {}",
                                if n.kind == explorer::Kind::View {
                                    "view"
                                } else {
                                    "table"
                                },
                                n.db
                            ),
                            action: PalAction::OpenTable(
                                n.db.clone(),
                                n.label.clone(),
                                n.kind == explorer::Kind::View,
                            ),
                        });
                    }
                }
            }
        }
        for q in &self.saved.items {
            out.push(PalItem {
                label: format!("{} {}", icons::glyph(Icon::Saved), q.name),
                hint: format!("saved · {}", q.lang),
                action: PalAction::Insert(q.text.clone()),
            });
        }
        self.history_items(&mut out);
        for (label, key, action) in [
            ("New tab", "Ctrl-T", PalAction::NewTab),
            ("Export the result", "Ctrl-E", PalAction::Export),
            ("Reload the explorer", "Ctrl-O", PalAction::Reload),
            ("Keys", "F1", PalAction::Help),
            ("Quit", "Ctrl-Q", PalAction::Quit),
        ] {
            out.push(PalItem {
                label: label.to_owned(),
                hint: format!("command · {key}"),
                action,
            });
        }
        out
    }

    fn palette_key(&mut self, q: &str, sel: usize, k: KeyEvent) -> Vec<Effect> {
        let mut q = q.to_owned();
        let shown = palette::filter(&self.palette_items(), &q, 50);
        let mut sel = sel.min(shown.len().saturating_sub(1));
        match k.code {
            KeyCode::Esc => {
                self.dialog = None;
                return Vec::new();
            }
            KeyCode::Up => sel = sel.saturating_sub(1),
            KeyCode::Down => sel = (sel + 1).min(shown.len().saturating_sub(1)),
            KeyCode::Backspace => {
                q.pop();
                sel = 0;
            }
            KeyCode::Enter => {
                self.dialog = None;
                return match shown.get(sel).map(|i| i.action.clone()) {
                    Some(a) => self.palette_act(a),
                    None => Vec::new(),
                };
            }
            KeyCode::Char(c) if !ctrl(&k) => {
                q.push(c);
                sel = 0;
            }
            _ => {}
        }
        self.dialog = Some(Dialog::Palette(q, sel));
        Vec::new()
    }

    fn palette_act(&mut self, a: PalAction) -> Vec<Effect> {
        match a {
            PalAction::Screen(i) => {
                self.screen = i.min(self.tabs.len() + FIXED.len() - 1);
                self.completion = None;
                self.entered()
            }
            PalAction::NewTab => {
                let lang = self.tabs.get(self.screen).map_or(Lang::Sql, |t| t.lang);
                self.status = String::from("opening a tab...");
                vec![Effect::NewTab(lang)]
            }
            PalAction::Help => {
                self.dialog = Some(Dialog::Help);
                Vec::new()
            }
            PalAction::Export => {
                if self.screen < self.tabs.len() {
                    self.dialog = Some(Dialog::Export);
                }
                Vec::new()
            }
            PalAction::Reload => vec![Effect::LoadCatalog],
            PalAction::OpenTable(db, table, is_view) => self.open_table(&db, &table, is_view),
            PalAction::Insert(text) => {
                if self.tabs.get(self.screen).is_none_or(|t| t.table.is_some()) {
                    let Some(i) = self.tabs.iter().position(|t| t.table.is_none()) else {
                        self.status = String::from("no query tab: Ctrl-T opens one");
                        return Vec::new();
                    };
                    self.screen = i;
                }
                if let Some(t) = self.tabs.get_mut(self.screen) {
                    t.editor.set_text(&text);
                    self.focus = Focus::Editor;
                    self.status = String::from("F5 runs it");
                }
                Vec::new()
            }
            PalAction::Quit => {
                self.quit = true;
                vec![Effect::Quit]
            }
        }
    }

    /// Checks statements and starts them, or asks first.
    pub fn submit(&mut self, stmts: Vec<String>) -> Vec<Effect> {
        let Some(tab) = self.tabs.get(self.screen) else {
            return Vec::new();
        };
        let (id, lang) = (tab.id, tab.lang);
        if tab.running {
            self.status = String::from("a statement is running; Ctrl-C cancels it");
            return Vec::new();
        }
        if stmts.is_empty() {
            return Vec::new();
        }
        let why: Vec<String> = stmts
            .iter()
            .filter_map(|s| {
                let d = destructive(s, lang);
                match self.confirm {
                    Confirm::Off => None,
                    Confirm::Destructive => d,
                    Confirm::Writes => d.or_else(|| {
                        (lang == Lang::Sql
                            && matches!(
                                classify_sql(s),
                                Class::Write | Class::Ddl | Class::Account
                            ))
                        .then(|| "a write".to_owned())
                    }),
                }
            })
            .collect();
        if !why.is_empty() {
            let shown: Vec<String> = stmts.iter().map(|s| redact(s, lang)).collect();
            self.dialog = Some(Dialog::Confirm(
                format!(
                    "{} — run it?\n\n{}",
                    why.join(", "),
                    crate::tui::clip(&shown.join(";\n"), 600)
                ),
                Pending::Run(id, stmts, lang),
            ));
            return Vec::new();
        }
        self.start_run(id, stmts, lang)
    }

    fn start_run(&mut self, id: TabId, stmts: Vec<String>, lang: Lang) -> Vec<Effect> {
        for s in &stmts {
            self.history.add(s, lang);
        }
        let Some(tab) = self.tab_by_id(id) else {
            return Vec::new();
        };
        let shown = redact(stmts.last().map_or("", String::as_str), lang);
        tab.last_run = Some((
            stmts
                .iter()
                .map(|s| redact(s.trim(), lang))
                .collect::<Vec<_>>()
                .join(";\n"),
            lang,
        ));
        tab.results.start(shown);
        tab.running = true;
        tab.started = Some(std::time::Instant::now());
        vec![Effect::Run(id, stmts, lang)]
    }

    /// Opens (or shows) a table tab for `db`.`table`.
    fn open_table(&mut self, db: &str, table: &str, is_view: bool) -> Vec<Effect> {
        self.focus = Focus::Results;
        if let Some(i) = self.tabs.iter().position(|t| {
            t.table
                .as_ref()
                .is_some_and(|v| v.db == db && v.table == table)
        }) {
            self.screen = i;
            return Vec::new();
        }
        let id = self.next_id;
        self.next_id += 1;
        let describe = self
            .tabs
            .get(self.screen)
            .map(|t| t.describe.clone())
            .unwrap_or_default();
        let mut t = DataTab::new(id, Lang::Sql, describe, Some(db.to_owned()));
        t.table = Some(Box::new(TableView::new(db, table, is_view)));
        self.tabs.push(t);
        self.screen = self.tabs.len() - 1;
        self.status = format!("{db}.{table}: / filter · e edit · a apply · [ ] tabs");
        vec![Effect::TableInfo(id, db.to_owned())]
    }

    fn table_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        let Some(t) = self.tabs.get_mut(self.screen) else {
            return Vec::new();
        };
        let id = t.id;
        let Some(v) = t.table.as_mut() else {
            return Vec::new();
        };
        let cmd = v.key(k);
        self.table_cmd(id, cmd)
    }

    fn table_view(&mut self, id: TabId) -> Option<&mut TableView> {
        self.tab_by_id(id).and_then(|t| t.table.as_deref_mut())
    }

    /// Carries out what a table view asked for.
    fn table_cmd(&mut self, id: TabId, cmd: TableCmd) -> Vec<Effect> {
        let Some(db) = self.table_view(id).map(|v| v.db.clone()) else {
            return Vec::new();
        };
        match cmd {
            TableCmd::None => Vec::new(),
            TableCmd::Query(p, sql) => vec![Effect::TableQuery(id, db, p, sql)],
            TableCmd::Shards => vec![Effect::TableShards(id)],
            TableCmd::EditFilter(cur) => {
                self.dialog = Some(Dialog::TableInput(TableField::Filter, cur));
                Vec::new()
            }
            TableCmd::EditCell(cur) => {
                self.dialog = Some(Dialog::TableInput(
                    TableField::Cell,
                    cur.unwrap_or_default(),
                ));
                Vec::new()
            }
            TableCmd::Apply(stmts) => {
                let shown: Vec<String> = stmts.iter().map(|s| format!("{};", s.sql)).collect();
                self.dialog = Some(Dialog::Confirm(
                    format!(
                        "Apply {} change{} in one transaction?\n\n{}",
                        stmts.len(),
                        if stmts.len() == 1 { "" } else { "s" },
                        crate::tui::clip_lines(&shown.join("\n"), 12)
                    ),
                    Pending::TableApply(id, db, stmts),
                ));
                Vec::new()
            }
            TableCmd::Copy(text, n) => {
                self.status = format!(
                    "copied {n} statement{} as SQL to the clipboard",
                    if n == 1 { "" } else { "s" }
                );
                vec![Effect::Copy(text)]
            }
            TableCmd::Status(s) => {
                self.status = s;
                Vec::new()
            }
        }
    }

    fn table_input_key(&mut self, field: TableField, text: &str, k: KeyEvent) -> Vec<Effect> {
        let mut text = text.to_owned();
        let id = self.tabs.get(self.screen).map_or(0, |t| t.id);
        match k.code {
            KeyCode::Esc => {
                self.dialog = None;
                return Vec::new();
            }
            _ if field == TableField::Cell && is_null_key(&k) => {
                self.dialog = None;
                if let Some(v) = self.table_view(id) {
                    let (r, c) = (v.row, v.col);
                    v.stage(r, c, None);
                }
                return Vec::new();
            }
            KeyCode::Enter => {
                self.dialog = None;
                let cmd = match self.table_view(id) {
                    None => TableCmd::None,
                    Some(v) if field == TableField::Filter => v.set_filter(&text),
                    Some(v) => {
                        let (r, c) = (v.row, v.col);
                        v.stage(r, c, Some(text));
                        TableCmd::None
                    }
                };
                return self.table_cmd(id, cmd);
            }
            KeyCode::Backspace => {
                text.pop();
            }
            KeyCode::Char(c) if !ctrl(&k) => text.push(c),
            _ => {}
        }
        self.dialog = Some(Dialog::TableInput(field, text));
        Vec::new()
    }

    /// Handles a table tab's message.
    #[allow(clippy::many_single_char_names)]
    fn table_msg(&mut self, m: Msg) -> Vec<Effect> {
        match m {
            Msg::TableInfo(id, r) => {
                let Some(v) = self.table_view(id) else {
                    return Vec::new();
                };
                match r {
                    Ok(snap) => {
                        v.cols = col_infos(&snap, &v.table.clone());
                        let cmd = v.reload();
                        return self.table_cmd(id, cmd);
                    }
                    Err(e) => {
                        v.loading = false;
                        v.error = Some(e.to_string());
                    }
                }
            }
            Msg::TableRows(id, p, r) => {
                let Some(v) = self.table_view(id) else {
                    return Vec::new();
                };
                match (p, r) {
                    (Purpose::Page, Ok(sets)) => {
                        let set = sets
                            .iter()
                            .find(|s| !s.columns.is_empty())
                            .cloned()
                            .unwrap_or_default();
                        v.set_page(set);
                    }
                    (Purpose::Page, Err(e)) => {
                        v.loading = false;
                        v.error = Some(e.to_string());
                    }
                    (Purpose::Structure(sub), r) => {
                        let shaped = r.map(|s| v.shape(sub, s)).map_err(|e| e.to_string());
                        v.other = Some((sub, shaped));
                    }
                }
            }
            Msg::TableShards(id, r) => {
                if let Some(v) = self.table_view(id) {
                    let shaped = r.map(|j| v.shards_set(&j)).map_err(|e| e.to_string());
                    v.other = Some((crate::tui::table::Sub::Shards, shaped));
                }
            }
            Msg::TableApplied(id, r) => {
                let Some(v) = self.table_view(id) else {
                    return Vec::new();
                };
                match r {
                    Ok(c) if c.is_empty() => {
                        let n = v.pending();
                        let cmd = v.reload();
                        self.status =
                            format!("applied {n} change{}", if n == 1 { "" } else { "s" });
                        return self.table_cmd(id, cmd);
                    }
                    Ok(c) => {
                        v.conflicts = c;
                        self.status = v.conflict_note();
                    }
                    Err(e) => self.status = format!("nothing applied: {e}"),
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn data_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        if self
            .tabs
            .get(self.screen)
            .is_some_and(|t| t.table.is_some())
        {
            match k.code {
                KeyCode::F(2) | KeyCode::Esc => {
                    self.focus = if self.focus == Focus::Explorer {
                        Focus::Results
                    } else {
                        Focus::Explorer
                    };
                    return Vec::new();
                }
                KeyCode::Char('o') if ctrl(&k) => return vec![Effect::LoadCatalog],
                _ if self.focus == Focus::Explorer => return self.explorer_key(k),
                _ => return self.table_key(k),
            }
        }
        // Keys for every pane of a data tab.
        match k.code {
            KeyCode::Char('c') if ctrl(&k) => {
                let Some(t) = self.tab_mut() else {
                    return Vec::new();
                };
                if t.running {
                    let id = t.id;
                    return vec![Effect::Cancel(id)];
                }
                t.editor = Editor::default();
                self.completion = None;
                return Vec::new();
            }
            KeyCode::F(2) => {
                self.focus = match self.focus {
                    Focus::Explorer => Focus::Editor,
                    Focus::Editor => Focus::Results,
                    Focus::Results => Focus::Explorer,
                };
                self.completion = None;
                return Vec::new();
            }
            KeyCode::F(3) => {
                if let Some(t) = self.tab_mut() {
                    t.results.view = t.results.view.next();
                    t.results.row = 0;
                    t.results.top = 0;
                }
                return Vec::new();
            }
            KeyCode::F(4) => {
                if let Some(t) = self.tab_mut() {
                    t.lang = match t.lang {
                        Lang::Sql => Lang::Redis,
                        Lang::Redis => Lang::Cypher,
                        Lang::Cypher => Lang::Mongo,
                        Lang::Mongo => Lang::Sql,
                    };
                    let l = t.lang.name();
                    self.status = format!("language: {l}");
                }
                return Vec::new();
            }
            KeyCode::F(5) => return self.run_at_cursor(),
            KeyCode::Char('r') if ctrl(&k) => return self.run_at_cursor(),
            KeyCode::Char('s') if ctrl(&k) => {
                self.dialog = Some(Dialog::SaveAs(String::new()));
                return Vec::new();
            }
            KeyCode::Char('e') if ctrl(&k) => {
                if self
                    .tabs
                    .get(self.screen)
                    .is_some_and(|t| t.results.set().is_some())
                {
                    self.dialog = Some(Dialog::Export);
                }
                return Vec::new();
            }
            KeyCode::Char('o') if ctrl(&k) => return vec![Effect::LoadCatalog],
            KeyCode::Esc => {
                if self.completion.take().is_none() {
                    self.focus = Focus::Editor;
                }
                return Vec::new();
            }
            _ => {}
        }
        match self.focus {
            Focus::Editor => self.editor_key(k),
            Focus::Explorer => self.explorer_key(k),
            Focus::Results => {
                self.results_key(k);
                Vec::new()
            }
        }
    }

    fn run_at_cursor(&mut self) -> Vec<Effect> {
        let Some(t) = self.tabs.get(self.screen) else {
            return Vec::new();
        };
        let s = t.editor.statement_at_cursor(t.lang);
        self.completion = None;
        if s.trim().is_empty() {
            return Vec::new();
        }
        self.submit(vec![s])
    }

    #[allow(clippy::too_many_lines)]
    fn editor_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        // The completion menu takes Up, Down, Enter and Tab first.
        if let Some(c) = &mut self.completion {
            match k.code {
                KeyCode::Up => {
                    c.selected = c.selected.saturating_sub(1);
                    return Vec::new();
                }
                KeyCode::Down => {
                    c.selected = (c.selected + 1).min(c.items.len().saturating_sub(1));
                    return Vec::new();
                }
                KeyCode::Enter | KeyCode::Tab => {
                    let (n, item) = (c.word_len, c.items[c.selected].clone());
                    self.completion = None;
                    if let Some(t) = self.tab_mut() {
                        t.editor.replace_before_cursor(n, &item);
                    }
                    return Vec::new();
                }
                _ => self.completion = None,
            }
        }
        let screen = self.screen;
        let Some(t) = self.tabs.get_mut(screen) else {
            return Vec::new();
        };
        let e = &mut t.editor;
        match k.code {
            KeyCode::Enter if alt(&k) => e.newline(),
            KeyCode::Enter => {
                if e.complete_at_end(t.lang) {
                    let text = e.text();
                    let stmts: Vec<String> = match split(&text, t.lang) {
                        Ok(s) => s.into_iter().map(|s| s.text).collect(),
                        Err(err) => {
                            self.status = err.to_string();
                            return Vec::new();
                        }
                    };
                    let fx = self.submit(stmts);
                    if !fx.is_empty()
                        && let Some(t) = self.tabs.get_mut(screen)
                    {
                        t.editor = Editor::default();
                    }
                    return fx;
                }
                e.newline();
            }
            KeyCode::Tab => {
                let word = e.word_before_cursor();
                if word.is_empty() {
                    e.insert_str("  ");
                    return Vec::new();
                }
                let stmt = {
                    let text = e.text();
                    text[..e.cursor_offset()].to_owned()
                };
                let items = complete(&self.catalog, t.lang, t.database.as_deref(), &stmt, &word);
                match items.len() {
                    0 => self.status = String::from("no completions"),
                    1 => e.replace_before_cursor(word.chars().count(), &items[0]),
                    _ => {
                        self.completion = Some(Completion {
                            items,
                            selected: 0,
                            word_len: word.chars().count(),
                        });
                    }
                }
            }
            KeyCode::Up if ctrl(&k) || alt(&k) => {
                if let Some(h) = self.history.older() {
                    let h = h.to_owned();
                    t.editor.set_text(&h);
                }
            }
            KeyCode::Down if ctrl(&k) || alt(&k) => {
                let h = self.history.newer().map(str::to_owned);
                t.editor.set_text(&h.unwrap_or_default());
            }
            KeyCode::Left if ctrl(&k) || alt(&k) => e.word_left(),
            KeyCode::Right if ctrl(&k) || alt(&k) => e.word_right(),
            KeyCode::Left => e.left(),
            KeyCode::Right => e.right(),
            KeyCode::Up => {
                e.up();
            }
            KeyCode::Down => {
                e.down();
            }
            KeyCode::Home => e.home(),
            KeyCode::End => e.end(),
            KeyCode::Backspace => e.backspace(),
            KeyCode::Delete => e.delete(),
            KeyCode::Char('a') if ctrl(&k) => e.home(),
            KeyCode::Char('u') if ctrl(&k) => e.kill_to_start(),
            KeyCode::Char('k') if ctrl(&k) => e.kill_to_end(),
            KeyCode::Char('l') if ctrl(&k) => t.editor = Editor::default(),
            KeyCode::Char(c) if !ctrl(&k) => e.insert(c),
            _ => {}
        }
        Vec::new()
    }

    fn explorer_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        let a = match k.code {
            KeyCode::Up => {
                self.explorer.move_by(-1);
                ExplorerAction::None
            }
            KeyCode::Down => {
                self.explorer.move_by(1);
                ExplorerAction::None
            }
            KeyCode::PageUp => {
                self.explorer.move_by(-10);
                ExplorerAction::None
            }
            KeyCode::PageDown => {
                self.explorer.move_by(10);
                ExplorerAction::None
            }
            KeyCode::Right => self.explorer.activate(false),
            KeyCode::Enter => self.explorer.activate(true),
            KeyCode::Left => {
                self.explorer.close();
                ExplorerAction::None
            }
            KeyCode::Char('r') => self
                .explorer
                .select_sql()
                .map_or(ExplorerAction::None, ExplorerAction::Run),
            _ => ExplorerAction::None,
        };
        match a {
            ExplorerAction::None => Vec::new(),
            ExplorerAction::Load(db) => vec![Effect::LoadDb(db)],
            ExplorerAction::Open(db, table, is_view) => self.open_table(&db, &table, is_view),
            ExplorerAction::Insert(s) => {
                if let Some(t) = self.tab_mut() {
                    t.editor.insert_str(&s);
                }
                self.focus = Focus::Editor;
                Vec::new()
            }
            ExplorerAction::Run(sql) => {
                if self.tab_mut().is_some_and(|t| t.table.is_some()) {
                    let Some(i) = self.tabs.iter().position(|t| t.table.is_none()) else {
                        self.status = String::from("no query tab: Ctrl-T opens one");
                        return Vec::new();
                    };
                    self.screen = i;
                }
                if let Some(t) = self.tab_mut() {
                    if t.lang != Lang::Sql {
                        t.lang = Lang::Sql;
                    }
                    t.editor = Editor::with_text(&sql);
                }
                self.focus = Focus::Results;
                self.submit(vec![sql.trim_end_matches(';').to_owned()])
            }
        }
    }

    fn results_key(&mut self, k: KeyEvent) {
        let Some(t) = self.tab_mut() else { return };
        let r = &mut t.results;
        match k.code {
            KeyCode::Up => r.move_rows(-1),
            KeyCode::Down => r.move_rows(1),
            KeyCode::PageUp => r.move_rows(-20),
            KeyCode::PageDown => r.move_rows(20),
            KeyCode::Home => r.row = 0,
            KeyCode::End => r.move_rows(isize::MAX / 2),
            KeyCode::Left => r.move_cols(-1),
            KeyCode::Right => r.move_cols(1),
            KeyCode::Char('[') => r.cycle_result(false),
            KeyCode::Char(']') => r.cycle_result(true),
            KeyCode::Enter => {
                if r.view == View::Json {
                    r.toggle_fold();
                } else if let Some(c) = r.selected_cell() {
                    self.dialog = Some(Dialog::Cell(c));
                }
            }
            _ => {}
        }
    }

    fn dash_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        let n = VIEW_NAMES.len();
        let view = |d: &DashState| VIEW_NAMES[d.view].to_owned();
        match k.code {
            KeyCode::Left => {
                self.dash.view = (self.dash.view + n - 1) % n;
                self.dash.row = 0;
                vec![Effect::DashRefresh(view(&self.dash))]
            }
            KeyCode::Right | KeyCode::Tab => {
                self.dash.view = (self.dash.view + 1) % n;
                self.dash.row = 0;
                vec![Effect::DashRefresh(view(&self.dash))]
            }
            KeyCode::Char(c @ '1'..='9') => {
                let i = (c as usize) - ('1' as usize);
                if i < n {
                    self.dash.view = i;
                    self.dash.row = 0;
                    return vec![Effect::DashRefresh(view(&self.dash))];
                }
                Vec::new()
            }
            KeyCode::Up => {
                self.dash.row = self.dash.row.saturating_sub(1);
                Vec::new()
            }
            KeyCode::Down => {
                self.dash.row += 1;
                Vec::new()
            }
            KeyCode::Char('r') => vec![Effect::DashRefresh(view(&self.dash))],
            KeyCode::Char(c @ ('k' | 'q')) if VIEW_NAMES[self.dash.view] == "sessions" => {
                let Some(Some(p)) = self.dash.panels.get(self.dash.view) else {
                    return Vec::new();
                };
                let Some(row) = p.rows.get(self.dash.row) else {
                    return Vec::new();
                };
                let id = row.first().cloned().unwrap_or_default();
                let what = if c == 'q' {
                    "the statement of session"
                } else {
                    "session"
                };
                self.dialog = Some(Dialog::Confirm(
                    format!("Kill {what} {id}?"),
                    Pending::Kill(id, c == 'q'),
                ));
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn admin_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        let a = &mut self.admin;
        match k.code {
            KeyCode::Enter => {
                let line = a.input.text().trim().to_owned();
                if line.is_empty() || a.running {
                    return Vec::new();
                }
                let line = line.strip_prefix("sprigctl ").unwrap_or(&line).to_owned();
                let args = match crate::admin::split_args(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        self.status = e.to_string();
                        return Vec::new();
                    }
                };
                a.history.push(line.clone());
                a.pos = a.history.len();
                a.input = Editor::default();
                a.scroll = 0;
                if admin_destructive(&args) && !args.iter().any(|x| x == "--yes") {
                    self.dialog = Some(Dialog::Confirm(
                        format!("sprigctl {line}\n\nThis changes the platform. Run it?"),
                        Pending::Admin(args),
                    ));
                    return Vec::new();
                }
                a.running = true;
                vec![Effect::Admin(args)]
            }
            KeyCode::Up => {
                if a.pos > 0 {
                    a.pos -= 1;
                    let h = a.history[a.pos].clone();
                    a.input.set_text(&h);
                }
                Vec::new()
            }
            KeyCode::Down => {
                if a.pos + 1 < a.history.len() {
                    a.pos += 1;
                    let h = a.history[a.pos].clone();
                    a.input.set_text(&h);
                } else {
                    a.pos = a.history.len();
                    a.input = Editor::default();
                }
                Vec::new()
            }
            KeyCode::PageUp => {
                a.scroll = (a.scroll + 10).min(a.output.len());
                Vec::new()
            }
            KeyCode::PageDown => {
                a.scroll = a.scroll.saturating_sub(10);
                Vec::new()
            }
            KeyCode::Left => {
                a.input.left();
                Vec::new()
            }
            KeyCode::Right => {
                a.input.right();
                Vec::new()
            }
            KeyCode::Backspace => {
                a.input.backspace();
                Vec::new()
            }
            KeyCode::Char('l') if ctrl(&k) => {
                a.output.clear();
                Vec::new()
            }
            KeyCode::Char('u') if ctrl(&k) => {
                a.input = Editor::default();
                Vec::new()
            }
            KeyCode::Char(c) if !ctrl(&k) => {
                a.input.insert(c);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn inbox_key(&mut self, k: KeyEvent) -> Vec<Effect> {
        match self.inbox.on_key(k) {
            crate::inbox::KeyOutcome::None => Vec::new(),
            crate::inbox::KeyOutcome::Refresh => vec![Effect::InboxRefresh],
            crate::inbox::KeyOutcome::Act(a) => vec![Effect::InboxAct(a)],
            crate::inbox::KeyOutcome::Confirm(q, a) => {
                self.dialog = Some(Dialog::Confirm(q, Pending::Inbox(a)));
                Vec::new()
            }
            crate::inbox::KeyOutcome::Ask(q, a) => {
                self.dialog = Some(Dialog::Input(q, String::new(), a));
                Vec::new()
            }
        }
    }

    /// Handles a runtime message.
    #[allow(clippy::too_many_lines)]
    pub fn on_msg(&mut self, m: Msg) -> Vec<Effect> {
        match m {
            Msg::Events(id, evs) => {
                if let Some(t) = self.tab_by_id(id) {
                    t.results.push(evs);
                }
            }
            Msg::Done {
                tab,
                error,
                elapsed_ms,
                describe,
                database,
                in_txn,
            } => {
                let db_changed;
                let shared = self.saved.shared;
                if let Some(t) = self.tab_by_id(tab) {
                    t.running = false;
                    t.started = None;
                    t.results.running = None;
                    db_changed = t.database != database;
                    t.describe = describe;
                    t.database = database;
                    t.in_txn = in_txn;
                    let rows: usize = t
                        .results
                        .sets
                        .iter()
                        .map(|s| s.rows.len().max(s.docs.len()))
                        .sum();
                    let truncated = t
                        .results
                        .sets
                        .iter()
                        .any(|s| s.end.as_ref().is_some_and(|e| e.truncated));
                    t.results.note = match &error {
                        Some(e) if e.is(crate::error::ShellCode::Cancelled) => {
                            "cancelled".to_owned()
                        }
                        Some(_) => format!("error · {elapsed_ms} ms"),
                        None => format!(
                            "{rows} row{}{} · {elapsed_ms} ms",
                            if rows == 1 { "" } else { "s" },
                            if truncated { " (cut at the cap)" } else { "" }
                        ),
                    };
                    let mut fx = Vec::new();
                    let cancelled = error
                        .as_ref()
                        .is_some_and(|e| e.is(crate::error::ShellCode::Cancelled));
                    if let Some((text, lang)) = t.last_run.take()
                        && shared
                        && !cancelled
                        && !text.is_empty()
                    {
                        fx.push(Effect::LibraryRecord(RunRecord {
                            lang,
                            database: t.database.clone(),
                            text,
                            ok: error.is_none(),
                            ms: u64::try_from(elapsed_ms).unwrap_or(u64::MAX),
                            error: error.as_ref().map(ToString::to_string),
                        }));
                    }
                    t.results.error = error;
                    if db_changed {
                        fx.push(Effect::LoadCatalog);
                    }
                    return fx;
                }
            }
            Msg::TabOpened(t) => {
                self.next_id = self.next_id.max(t.id + 1);
                self.tabs.push(*t);
                self.screen = self.tabs.len() - 1;
                self.status = String::from("new tab");
            }
            Msg::Catalog(r) => match r {
                Ok(v) => {
                    self.catalog.databases = v
                        .get("databases")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        })
                        .unwrap_or_default();
                    self.explorer.set_databases(&v);
                    // Load the current database so completion knows it.
                    if let Some(db) = self.tabs.get(self.screen).and_then(|t| t.database.clone())
                        && !self.catalog.objects.contains_key(&db)
                    {
                        return vec![Effect::LoadDb(db)];
                    }
                }
                Err(e) => self.explorer.note = Some(format!("catalog: {e}")),
            },
            Msg::Db(db, r, streams, sinks) => match r {
                Ok(v) => {
                    self.catalog.set_snapshot(&db, &v, &streams);
                    self.explorer.set_database(&db, &v, &streams, &sinks);
                }
                Err(e) => self.explorer.set_error(&db, &e.to_string()),
            },
            Msg::Dash(p) => {
                if let Some(i) = VIEW_NAMES.iter().position(|v| *v == p.view) {
                    self.dash.panels[i] = Some(p);
                    if i == self.dash.view {
                        self.dash.polled = Some(std::time::Instant::now());
                    }
                }
            }
            Msg::Admin(args, r) => {
                self.admin.running = false;
                self.admin.output.push(Line::styled(
                    format!("sprigctl {}", args.join(" ")),
                    theme::accent().add_modifier(Modifier::BOLD),
                ));
                match r {
                    Ok(c) => {
                        for l in c.stdout.lines() {
                            self.admin.output.push(Line::from(l.to_owned()));
                        }
                        for l in c.stderr.lines() {
                            self.admin
                                .output
                                .push(Line::styled(l.to_owned(), theme::error()));
                        }
                        if c.code != 0 {
                            self.admin
                                .output
                                .push(Line::styled(format!("(exit {})", c.code), theme::error()));
                        }
                    }
                    Err(e) => self
                        .admin
                        .output
                        .push(Line::styled(e.to_string(), theme::error())),
                }
                let keep = 5000;
                if self.admin.output.len() > keep {
                    let d = self.admin.output.len() - keep;
                    self.admin.output.drain(..d);
                }
            }
            Msg::Inbox(r) => self.inbox.set(r),
            // A gateway that can't keep them (older, or an accounts-file
            // login): the profile's file stays the only copy.
            Msg::Library(Ok(v)) => {
                return self
                    .saved
                    .adopt(&v)
                    .into_iter()
                    .map(Effect::LibrarySave)
                    .collect();
            }
            Msg::Library(Err(_)) => {}
            m @ (Msg::TableInfo(..)
            | Msg::TableRows(..)
            | Msg::TableShards(..)
            | Msg::TableApplied(..)) => return self.table_msg(m),
            Msg::Status(s) => self.status = s,
        }
        Vec::new()
    }

    /// Effects of the refresh timer: the visible dashboard view.
    #[must_use]
    pub fn tick(&self, refresh: std::time::Duration) -> Vec<Effect> {
        match self.fixed() {
            Some(Fixed::Dash) if self.dash.polled.is_none_or(|t| t.elapsed() >= refresh) => {
                vec![Effect::DashRefresh(VIEW_NAMES[self.dash.view].to_owned())]
            }
            _ => Vec::new(),
        }
    }

    /// Draws everything.
    pub fn draw(&mut self, f: &mut Frame<'_>) {
        let area = f.area();
        if area.width < 60 || area.height < 16 {
            f.render_widget(
                Paragraph::new(format!(
                    "The terminal is {}x{}; sprig needs at least 60x16.",
                    area.width, area.height
                ))
                .wrap(Wrap { trim: true }),
                area,
            );
            return;
        }
        let [top, body, bottom] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .areas(area);
        self.draw_tabs(f, top);
        match self.fixed() {
            None => self.draw_data(f, body),
            Some(Fixed::Dash) => self.draw_dash(f, body),
            Some(Fixed::Admin) => self.draw_admin(f, body),
            Some(Fixed::Inbox) => self.inbox.draw(f, body),
        }
        self.draw_status(f, bottom);
        self.draw_dialog(f, area);
    }

    fn draw_tabs(&self, f: &mut Frame<'_>, area: Rect) {
        let mut titles: Vec<Line<'_>> = self
            .tabs
            .iter()
            .enumerate()
            .map(|(i, t)| {
                if let Some(v) = &t.table {
                    let busy = if v.loading { " …" } else { "" };
                    let dirty = if v.pending() > 0 { " ●" } else { "" };
                    return Line::from(format!(
                        "{} {} {}{dirty}{busy}",
                        icons::glyph(Icon::Table),
                        i + 1,
                        v.table
                    ));
                }
                let db = t.database.as_deref().unwrap_or("-");
                let busy = if t.running { " …" } else { "" };
                Line::from(format!(
                    "{} {} {} {db}{busy}",
                    icons::glyph(Icon::Query),
                    i + 1,
                    t.lang.name()
                ))
            })
            .collect();
        titles.push(Line::from(format!(
            "{} Dashboard",
            icons::glyph(Icon::Dashboard)
        )));
        titles.push(Line::from(format!("{} Admin", icons::glyph(Icon::Admin))));
        titles.push(Line::from(match self.inbox.count() {
            Some(n) if n > 0 => format!("{} Inbox ({n})", icons::glyph(Icon::Inbox)),
            _ => format!("{} Inbox", icons::glyph(Icon::Inbox)),
        }));
        f.render_widget(
            Tabs::new(titles)
                .select(self.screen)
                .highlight_style(theme::tab_active())
                .divider(Span::styled("│", theme::border())),
            area,
        );
    }

    fn pane(title: String, focused: bool) -> Block<'static> {
        let style = if focused {
            theme::focus_border()
        } else {
            theme::border()
        };
        Block::bordered().border_style(style).title(title)
    }

    #[allow(clippy::many_single_char_names)]
    fn draw_data(&mut self, f: &mut Frame<'_>, area: Rect) {
        let show_explorer = area.width >= 90;
        let [left, right] = if show_explorer {
            Layout::horizontal([Constraint::Length(30), Constraint::Min(30)]).areas(area)
        } else {
            [Rect::default(), area]
        };
        if show_explorer {
            let block = Self::pane(" Explorer ".to_owned(), self.focus == Focus::Explorer);
            let inner = block.inner(left);
            let lines = self
                .explorer
                .lines(inner.height as usize, self.focus == Focus::Explorer);
            f.render_widget(Paragraph::new(lines).block(block), left);
        }
        let screen = self.screen;
        let focus = self.focus;
        let Some(t) = self.tabs.get_mut(screen) else {
            return;
        };
        if let Some(v) = t.table.as_deref_mut() {
            v.draw(f, right, focus != Focus::Explorer);
            return;
        }
        let ed_h = (u16::try_from(t.editor.line_count()).unwrap_or(u16::MAX) + 2)
            .clamp(5, (right.height / 2).max(5));
        let [ed_area, res_area] =
            Layout::vertical([Constraint::Length(ed_h), Constraint::Min(3)]).areas(right);
        let block = Self::pane(
            format!(" Editor [{}] ", t.lang.name()),
            focus == Focus::Editor,
        );
        let inner = block.inner(ed_area);
        t.editor.scroll_to_cursor(inner.height as usize);
        let lines = t.editor.render_lines(t.lang, inner.height as usize);
        f.render_widget(Paragraph::new(lines).block(block), ed_area);
        if focus == Focus::Editor && self.dialog.is_none() {
            let x = inner.x
                + u16::try_from(t.editor.gutter() + t.editor.cursor_display_col()).unwrap_or(0);
            let y =
                inner.y + u16::try_from(t.editor.row.saturating_sub(t.editor.scroll)).unwrap_or(0);
            if x < inner.right() && y < inner.bottom() {
                f.set_cursor_position((x, y));
            }
            if let Some(c) = &self.completion {
                let h = u16::try_from(c.items.len().min(8)).unwrap_or(8) + 2;
                let w = u16::try_from(c.items.iter().map(String::len).max().unwrap_or(4) + 4)
                    .unwrap_or(40)
                    .min(40);
                let popup = Rect {
                    x: x.min(area.right().saturating_sub(w)),
                    y: (y + 1).min(area.bottom().saturating_sub(h)),
                    width: w,
                    height: h,
                };
                let first = c.selected.saturating_sub(7);
                let items: Vec<Line<'_>> = c
                    .items
                    .iter()
                    .enumerate()
                    .skip(first)
                    .take(8)
                    .map(|(i, s)| {
                        let st = if i == c.selected {
                            theme::selected()
                        } else {
                            Style::new()
                        };
                        Line::styled(s.clone(), st)
                    })
                    .collect();
                f.render_widget(Clear, popup);
                f.render_widget(Paragraph::new(items).block(Block::bordered()), popup);
            }
        }
        let title = t.results.title();
        let block = Self::pane(title, focus == Focus::Results);
        t.results.draw(f, res_area, block);
    }

    fn draw_dash(&mut self, f: &mut Frame<'_>, area: Rect) {
        let [left, right] =
            Layout::horizontal([Constraint::Length(18), Constraint::Min(30)]).areas(area);
        let views: Vec<Line<'_>> = VIEW_NAMES
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let st = if i == self.dash.view {
                    theme::selected()
                } else {
                    Style::new()
                };
                Line::styled(format!("{} {v}", i + 1), st)
            })
            .collect();
        f.render_widget(
            Paragraph::new(views).block(Block::bordered().title(" Views ")),
            left,
        );
        let name = VIEW_NAMES[self.dash.view];
        let age = self.dash.polled.map_or_else(
            || "not polled yet".to_owned(),
            |t| format!("{} s ago", t.elapsed().as_secs()),
        );
        let block = Block::bordered().title(format!(" {name} · {age} · r refresh "));
        let inner = block.inner(right);
        f.render_widget(block, right);
        let Some(Some(p)) = self.dash.panels.get(self.dash.view).cloned() else {
            f.render_widget(Paragraph::new("loading..."), inner);
            return;
        };
        let [note_area, table_area] = if p.note.is_some() {
            Layout::vertical([Constraint::Length(2), Constraint::Min(1)]).areas(inner)
        } else {
            [Rect::default(), inner]
        };
        if let Some(n) = &p.note {
            f.render_widget(
                Paragraph::new(Line::styled(n.clone(), theme::warn())).wrap(Wrap { trim: true }),
                note_area,
            );
        }
        if p.columns.is_empty() {
            return;
        }
        self.dash.row = self.dash.row.min(p.rows.len().saturating_sub(1));
        let widths: Vec<Constraint> = p
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = p
                    .rows
                    .iter()
                    .take(100)
                    .filter_map(|r| r.get(i))
                    .map(|v| v.chars().count())
                    .chain([c.chars().count()])
                    .max()
                    .unwrap_or(4)
                    .min(36);
                Constraint::Length(u16::try_from(w).unwrap_or(36))
            })
            .collect();
        let h = table_area.height.saturating_sub(1) as usize;
        let first = self.dash.row.saturating_sub(h.saturating_sub(1));
        let rows = p.rows.iter().enumerate().skip(first).take(h).map(|(i, r)| {
            let row = Row::new(r.iter().map(|c| crate::tui::clip(c, 36)));
            if i == self.dash.row {
                row.style(theme::selected())
            } else {
                row
            }
        });
        let header = Row::new(p.columns.clone()).style(Style::new().add_modifier(Modifier::BOLD));
        f.render_widget(
            Table::new(rows, widths).header(header).column_spacing(1),
            table_area,
        );
    }

    fn draw_admin(&self, f: &mut Frame<'_>, area: Rect) {
        let [out_area, in_area] =
            Layout::vertical([Constraint::Min(3), Constraint::Length(3)]).areas(area);
        let block = Block::bordered().title(" Admin · sprigctl output · PgUp/PgDn ");
        let inner = block.inner(out_area);
        let h = inner.height as usize;
        let end = self.admin.output.len().saturating_sub(self.admin.scroll);
        let start = end.saturating_sub(h);
        let lines: Vec<Line<'static>> = if self.admin.output.is_empty() {
            vec![
                Line::from(
                    "Type a sprigctl command without \"sprigctl\": tenant list, cluster show c1,",
                ),
                Line::from("settings get gateway.max_connections --scope global, job list ..."),
                Line::styled(
                    if self.operator {
                        "Commands run as sprigctl with this profile's admin_url and admin_token_file."
                    } else {
                        "This profile has no admin_url: sprigctl uses its own defaults (127.0.0.1:7480)."
                    },
                    theme::muted(),
                ),
            ]
        } else {
            self.admin.output[start..end].to_vec()
        };
        f.render_widget(Paragraph::new(lines).block(block), out_area);
        let title = if self.admin.running {
            " sprigctl (running) "
        } else {
            " sprigctl "
        };
        let block = Block::bordered()
            .title(title)
            .border_style(theme::focus_border());
        let inner = block.inner(in_area);
        f.render_widget(
            Paragraph::new(self.admin.input.text()).block(block),
            in_area,
        );
        if self.dialog.is_none() {
            let x = inner.x + u16::try_from(self.admin.input.cursor_display_col()).unwrap_or(0);
            f.set_cursor_position((x.min(inner.right().saturating_sub(1)), inner.y));
        }
    }

    fn draw_status(&self, f: &mut Frame<'_>, area: Rect) {
        let mut spans = Vec::new();
        if let Some(t) = self
            .tabs
            .get(self.screen.min(self.tabs.len().saturating_sub(1)))
        {
            spans.push(Span::styled(
                format!(" {} ", self.profile),
                theme::selected(),
            ));
            spans.push(Span::raw(format!(" {} ", t.describe)));
            if t.in_txn {
                spans.push(Span::styled(" TXN ", theme::warn_badge()));
            }
            if let Some(s) = t.started {
                spans.push(Span::raw(format!(
                    " running {} ms ",
                    s.elapsed().as_millis()
                )));
            }
        }
        if let Some(n) = self.inbox.count().filter(|n| *n > 0) {
            spans.push(Span::styled(format!(" ⚑ {n} to approve "), theme::badge()));
        }
        spans.push(Span::styled(format!(" {}", self.status), theme::muted()));
        f.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    #[allow(clippy::many_single_char_names, clippy::too_many_lines)]
    fn draw_dialog(&self, f: &mut Frame<'_>, area: Rect) {
        let Some(d) = &self.dialog else { return };
        let (title, body): (&str, Vec<Line<'_>>) = match d {
            Dialog::Confirm(q, _) => {
                let mut l: Vec<Line<'_>> = q.lines().map(|s| Line::from(s.to_owned())).collect();
                l.push(Line::from(""));
                l.push(Line::styled("y: run it    n or Esc: don't", theme::warn()));
                (" Confirm ", l)
            }
            Dialog::Help => (" Keys ", help_lines()),
            Dialog::Cell(s) => (
                " Value · any key closes ",
                s.lines().map(|l| Line::from(l.to_owned())).collect(),
            ),
            Dialog::Export => (
                " Export the shown result ",
                vec![Line::from(
                    "c: CSV   j: JSON   n: NDJSON   t: TSV   Esc: cancel",
                )],
            ),
            Dialog::Palette(q, sel) => {
                let shown = palette::filter(&self.palette_items(), q, 14);
                let mut l = vec![Line::from(vec![
                    Span::styled("› ", theme::accent()),
                    Span::raw(q.clone()),
                    Span::styled("▏", theme::accent()),
                ])];
                l.push(Line::from(""));
                if shown.is_empty() {
                    l.push(Line::styled("Nothing matches.", theme::muted()));
                }
                for (i, it) in shown.iter().enumerate() {
                    let row = Line::from(vec![
                        Span::raw(format!("{:<44}", crate::tui::clip(&it.label, 44))),
                        Span::styled(it.hint.clone(), theme::muted()),
                    ]);
                    l.push(if i == *sel {
                        row.style(theme::selected())
                    } else {
                        row
                    });
                }
                (" Search · Enter opens · Esc closes ", l)
            }
            Dialog::SaveAs(name) => (
                " Save the editor's text ",
                vec![
                    Line::from(vec![
                        Span::styled("Name › ", theme::accent()),
                        Span::raw(name.clone()),
                    ]),
                    Line::styled(
                        "Enter: save (Ctrl-K finds it later)   Esc: cancel",
                        theme::muted(),
                    ),
                ],
            ),
            Dialog::TableInput(field, text) => {
                let (title, prompt, hint) = match field {
                    TableField::Filter => (
                        " Filter ",
                        "WHERE › ",
                        "Enter: apply (empty clears)   Esc: cancel",
                    ),
                    TableField::Cell => (
                        " New value ",
                        "› ",
                        "Enter: stage it   Ctrl-N: NULL   Esc: cancel",
                    ),
                };
                (
                    title,
                    vec![
                        Line::from(vec![
                            Span::styled(prompt, theme::accent()),
                            Span::raw(text.clone()),
                        ]),
                        Line::styled(hint, theme::muted()),
                    ],
                )
            }
            Dialog::Input(q, text, _) => (
                " Inbox ",
                vec![
                    Line::from(q.clone()),
                    Line::from(format!("> {text}")),
                    Line::styled("Enter: send   Esc: cancel", theme::muted()),
                ],
            ),
        };
        let w = area.width.saturating_sub(10).min(84);
        let h = (u16::try_from(body.len()).unwrap_or(20) + 2).min(area.height.saturating_sub(4));
        let r = Rect {
            x: area.x + (area.width - w) / 2,
            y: area.y + (area.height - h) / 2,
            width: w,
            height: h,
        };
        f.render_widget(Clear, r);
        f.render_widget(
            Paragraph::new(body).wrap(Wrap { trim: false }).block(
                Block::bordered()
                    .title(title)
                    .border_style(if matches!(d, Dialog::Confirm(..)) {
                        theme::warn()
                    } else {
                        theme::focus_border()
                    }),
            ),
            r,
        );
    }
}

fn help_lines() -> Vec<Line<'static>> {
    [
        "F5, Ctrl-R    run the statement at the cursor",
        "Enter         after ';' at the end: run everything typed",
        "Alt-Enter     new line",
        "Tab           complete (Up/Down, Enter)",
        "Ctrl-Up/Down  history (secrets are stored redacted)",
        "Ctrl-C        cancel the running statement, else clear",
        "F2            focus: explorer, editor, results",
        "F3            results as grid, JSON tree or graph",
        "F4            language: sql, redis, cypher, mongo",
        "[ ]           previous or next result",
        "Enter         (results) show a value / fold JSON",
        "Ctrl-E        export the result: CSV, JSON, NDJSON, TSV",
        "Ctrl-O        reload the explorer",
        "Ctrl-T, Ctrl-W  new tab, close tab",
        "F6, F7, Alt-n previous or next tab, tab n",
        "Dashboard     1-6 or Left/Right views, r refresh, k kill, q kill query",
        "Admin         sprigctl commands; Up/Down history",
        "Inbox         a approve, w window, i ignore, s snooze, c comment",
        "Ctrl-K, Ctrl-P search tabs, tables, saved queries, history, commands",
        "Ctrl-S        save the editor's text under a name",
        "Enter, r      (explorer) open a table's view, or run a SELECT on it",
        "Table view    / filter, = by cell, e edit, o add, d delete, n p page",
        "              a apply, r reset, y copy as SQL, t take current, [ ] tabs",
        "Ctrl-Q        quit",
    ]
    .into_iter()
    .map(Line::from)
    .collect()
}
