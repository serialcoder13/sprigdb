//! The full-screen TUI (chapter 53, "The TUI"): the runtime that carries
//! out [`app::Effect`]s and feeds [`app::Msg`]s back.
//!
//! Statements run in tasks holding their tab's [`Runner`]; `Ctrl-C` aborts
//! the task, which drops the REST stream (the gateway kills the statement)
//! or, natively, sends `KILL QUERY` and reconnects.

pub mod app;
pub mod complete;
pub mod editor;
pub mod explorer;
pub mod icons;
pub mod mascot;
pub mod palette;
pub mod results;
pub mod table;
pub mod theme;

use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste, Event, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{Mutex, mpsc};
use tokio::task::AbortHandle;
use unicode_width::UnicodeWidthChar;

use crate::admin::AdminApi;
use crate::api::{Caps, DataClient};
use crate::cli::Ctx;
use crate::dash::{Rates, Source};
use crate::error::{ShellCode, ShellError};
use crate::exec::Runner;
use crate::history::History;
use crate::inbox::InboxSource;
use crate::lang::Lang;
use crate::model::{Collect, ResultEvent, ResultSet, Sink};
use crate::native::{Canceller, Native, NativeTarget, Protocol};
use crate::output::Writer;
use app::{App, DataTab, Effect, Fixed, Msg, TabId};

/// `s` cut to `w` display columns, with `…` when cut.
#[must_use]
pub fn clip_lines(s: &str, n: usize) -> String {
    let total = s.lines().count();
    let mut out: Vec<&str> = s.lines().take(n).collect();
    let more = total.saturating_sub(n);
    let tail = format!("... and {more} more");
    if more > 0 {
        out.push(&tail);
    }
    out.join("\n")
}

/// `s` on one line, cut to `w` columns with an ellipsis.
#[must_use]
pub fn clip(s: &str, w: usize) -> String {
    let s = s.replace(['\n', '\r', '\t'], " ");
    if unicode_width::UnicodeWidthStr::width(s.as_str()) <= w {
        return s;
    }
    let mut out = String::new();
    let mut used = 0;
    for c in s.chars() {
        let cw = c.width().unwrap_or(0);
        if used + cw + 1 > w {
            break;
        }
        used += cw;
        out.push(c);
    }
    out.push('…');
    out
}

/// Sends a tab's events in batches.
struct ChannelSink {
    tab: TabId,
    tx: mpsc::UnboundedSender<Msg>,
    buf: Vec<ResultEvent>,
    rows: usize,
}

impl ChannelSink {
    fn flush(&mut self) {
        if !self.buf.is_empty() {
            let _ = self
                .tx
                .send(Msg::Events(self.tab, std::mem::take(&mut self.buf)));
            self.rows = 0;
        }
    }
}

impl Sink for ChannelSink {
    fn event(&mut self, ev: ResultEvent) -> std::io::Result<()> {
        let flush = match &ev {
            ResultEvent::Rows(r) => {
                self.rows += r.len();
                self.rows >= 500
            }
            _ => true,
        };
        self.buf.push(ev);
        if flush {
            self.flush();
        }
        Ok(())
    }
}

/// A running statement: its task, how to cancel it, and the statement it
/// is on (for a REST cancel).
type Task = (
    AbortHandle,
    Option<Canceller>,
    Arc<std::sync::Mutex<String>>,
);

/// The runtime's state beside the [`App`].
struct Rt {
    ctx: Arc<Ctx>,
    protocol: Protocol,
    caps: Caps,
    client: DataClient,
    target: Option<NativeTarget>,
    runners: HashMap<TabId, Arc<Mutex<Runner>>>,
    tasks: HashMap<TabId, Task>,
    tx: mpsc::UnboundedSender<Msg>,
    admin: Option<AdminApi>,
    rates: Rates,
    last_auth: Instant,
}

fn describe(r: &Runner) -> (String, Option<String>, bool) {
    let txn = r.txn.is_some() || r.native.as_ref().is_some_and(Native::in_transaction);
    (r.describe(), r.database.clone(), txn)
}

fn first_column(evs: &[ResultEvent]) -> Vec<String> {
    ResultSet::from_events(evs)
        .into_iter()
        .flat_map(|s| {
            let i = s
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case("name"))
                .unwrap_or(0);
            s.rows
                .into_iter()
                .filter_map(move |r| r.get(i).cloned().flatten())
        })
        .collect()
}

impl Rt {
    fn src(&self, app: &App) -> Source {
        let db = app
            .tabs
            .get(app.screen.min(app.tabs.len().saturating_sub(1)))
            .and_then(|t| t.database.clone());
        Source {
            client: self.client.clone(),
            database: db.or_else(|| self.ctx.profile.database.clone()),
            admin: self.admin.clone(),
            tenant: self.ctx.profile.tenant.clone(),
        }
    }

    /// Re-reads the session's access token about once a minute, so a long
    /// TUI session outlives `auth.token_ttl_seconds` (the signer refreshes
    /// under the profile's lock when it is near expiry).
    async fn keep_signed_in(&mut self) {
        if self.protocol != Protocol::Http || self.last_auth.elapsed() < Duration::from_secs(60) {
            return;
        }
        self.last_auth = Instant::now();
        let signer = self.ctx.signer();
        let mut no_prompt = |_: &str| None;
        if let Ok(auth) = signer.auth(self.client.http(), &mut no_prompt).await {
            self.client.auth = auth.clone();
            for r in self.runners.values() {
                if let Ok(mut g) = r.try_lock() {
                    g.client.auth = auth.clone();
                }
            }
        }
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    fn apply(&mut self, app: &mut App, e: Effect) -> bool {
        let tx = self.tx.clone();
        match e {
            Effect::Quit => return false,
            Effect::Run(tab, stmts, lang) => {
                let Some(r) = self.runners.get(&tab).cloned() else {
                    return true;
                };
                let canceller = r
                    .try_lock()
                    .ok()
                    .and_then(|g| g.native.as_ref().and_then(Native::canceller));
                let current = Arc::new(std::sync::Mutex::new(String::new()));
                let cur = Arc::clone(&current);
                let h = tokio::spawn(async move {
                    let start = Instant::now();
                    let mut g = r.lock().await;
                    let mut sink = ChannelSink {
                        tab,
                        tx: tx.clone(),
                        buf: Vec::new(),
                        rows: 0,
                    };
                    let mut error = None;
                    for s in stmts {
                        if let Ok(mut c) = cur.lock() {
                            c.clone_from(&s);
                        }
                        if let Err(e) = g.run(&s, lang, &mut sink).await {
                            error = Some(e);
                            break;
                        }
                    }
                    sink.flush();
                    let (describe, database, in_txn) = describe(&g);
                    let _ = tx.send(Msg::Done {
                        tab,
                        error,
                        elapsed_ms: start.elapsed().as_millis(),
                        describe,
                        database,
                        in_txn,
                    });
                });
                let canceller = canceller.or_else(|| {
                    (self.protocol == Protocol::Http && lang == Lang::Sql).then(|| {
                        Canceller::RestKill {
                            client: self.client.clone(),
                            sql: String::new(),
                        }
                    })
                });
                self.tasks
                    .insert(tab, (h.abort_handle(), canceller, current));
            }
            Effect::Cancel(tab) => {
                if let Some((h, canceller, current)) = self.tasks.remove(&tab) {
                    h.abort();
                    let canceller = match canceller {
                        Some(Canceller::RestKill { client, .. }) => Some(Canceller::RestKill {
                            client,
                            sql: current.lock().map(|c| c.clone()).unwrap_or_default(),
                        }),
                        other => other,
                    };
                    let r = self.runners.get(&tab).cloned();
                    tokio::spawn(async move {
                        if let Some(c) = canceller {
                            let _ = c.cancel().await;
                        }
                        let mut info = (String::new(), None, false);
                        if let Some(r) = r {
                            let mut g = r.lock().await;
                            if let Some(n) = g.native.as_mut() {
                                let _ = n.reset().await;
                            }
                            info = describe(&g);
                        }
                        let _ = tx.send(Msg::Done {
                            tab,
                            error: Some(ShellError::shell(ShellCode::Cancelled, "cancelled")),
                            elapsed_ms: 0,
                            describe: info.0,
                            database: info.1,
                            in_txn: info.2,
                        });
                    });
                }
            }
            Effect::NewTab(lang) => {
                let id = app.next_id;
                app.next_id += 1;
                let mut runner = Runner::new(
                    self.client.clone(),
                    self.ctx.profile.database.clone(),
                    self.caps,
                );
                let target = self.target.clone();
                let protocol = self.protocol;
                let runners_entry = Arc::new(Mutex::new(Runner::new(
                    self.client.clone(),
                    None,
                    self.caps,
                )));
                self.runners.insert(id, Arc::clone(&runners_entry));
                tokio::spawn(async move {
                    if let Some(t) = target {
                        match Native::connect(protocol, t).await {
                            Ok(n) => runner.native = Some(n),
                            Err(e) => {
                                let _ = tx.send(Msg::Status(format!("new tab: {e}")));
                                return;
                            }
                        }
                    }
                    let d = runner.describe();
                    let db = runner.database.clone();
                    *runners_entry.lock().await = runner;
                    let _ = tx.send(Msg::TabOpened(Box::new(DataTab::new(id, lang, d, db))));
                });
            }
            Effect::CloseTab(id) => {
                if let Some((h, _, _)) = self.tasks.remove(&id) {
                    h.abort();
                }
                self.runners.remove(&id);
            }
            Effect::LoadCatalog => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    let _ = tx.send(Msg::Catalog(c.catalog().await));
                });
            }
            Effect::LoadDb(db) => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    let snap = c.database_catalog(&db).await;
                    let q = format!("`{}`", db.replace('`', "``"));
                    let mut s = Collect::default();
                    let _ = c
                        .sql_query(
                            Some(&db),
                            &format!("SHOW STREAMS FROM {q}"),
                            None,
                            Caps::default(),
                            &mut s,
                        )
                        .await;
                    let mut k = Collect::default();
                    let _ = c
                        .sql_query(
                            Some(&db),
                            &format!("SHOW SINKS FROM {q}"),
                            None,
                            Caps::default(),
                            &mut k,
                        )
                        .await;
                    let _ = tx.send(Msg::Db(db, snap, first_column(&s.0), first_column(&k.0)));
                });
            }
            Effect::DashRefresh(view) => {
                let src = self.src(app);
                app.dash.polled = Some(Instant::now());
                tokio::spawn(async move {
                    let p = crate::dash::fetch(&src, &view).await;
                    let _ = tx.send(Msg::Dash(p));
                });
            }
            Effect::Kill(id, q) => {
                let src = self.src(app);
                tokio::spawn(async move {
                    let s = match crate::dash::kill(&src, &id, q).await {
                        Ok(()) => format!("killed {}{id}", if q { "the query of " } else { "" }),
                        Err(e) => format!("kill {id}: {e}"),
                    };
                    let _ = tx.send(Msg::Status(s));
                    let _ = tx.send(Msg::Dash(crate::dash::fetch(&src, "sessions").await));
                });
            }
            Effect::Admin(args) => {
                let profile = self.ctx.profile.clone();
                tokio::spawn(async move {
                    let r = match crate::admin::find_sprigctl() {
                        Ok(exe) => crate::admin::capture_sprigctl(&exe, &args, &profile).await,
                        Err(e) => Err(e),
                    };
                    let _ = tx.send(Msg::Admin(args, r));
                });
            }
            Effect::Export(tab, format) => {
                let msg = export(app, tab, format);
                app.status = msg;
            }
            Effect::LoadLibrary => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    let _ = tx.send(Msg::Library(c.library(200).await));
                });
            }
            Effect::LibrarySave(q) => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    if let Err(e) = c
                        .library_save(&q.name, &q.lang, q.database.as_deref(), &q.text)
                        .await
                    {
                        let _ = tx.send(Msg::Status(format!(
                            "\"{}\" is saved here; the catalog didn't take it: {e}",
                            q.name
                        )));
                    }
                });
            }
            Effect::LibraryRecord(run) => {
                let c = self.client.clone();
                // History is best effort: a failure isn't worth a status line.
                tokio::spawn(async move {
                    let _ = c.library_record(&run.body()).await;
                });
            }
            Effect::TableInfo(id, db) => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    let _ = tx.send(Msg::TableInfo(id, c.database_catalog(&db).await));
                });
            }
            Effect::TableQuery(id, db, p, sql) => {
                let (c, caps) = (self.client.clone(), self.caps);
                tokio::spawn(async move {
                    let mut s = Collect::default();
                    let r = c
                        .query_in(Some(&db), "sql", &sql, None, caps, &mut s)
                        .await
                        .map(|()| ResultSet::from_events(&s.0));
                    let _ = tx.send(Msg::TableRows(id, p, r));
                });
            }
            Effect::TableShards(id) => {
                let c = self.client.clone();
                tokio::spawn(async move {
                    let _ = tx.send(Msg::TableShards(id, c.get("/v1/shards").await));
                });
            }
            Effect::TableApply(id, db, stmts) => {
                let (c, caps) = (self.client.clone(), self.caps);
                tokio::spawn(async move {
                    let r = table_apply(&c, &db, &stmts, caps).await;
                    let _ = tx.send(Msg::TableApplied(id, r));
                });
            }
            Effect::Copy(text) => copy_osc52(&text),
            Effect::InboxRefresh => {
                let src = InboxSource {
                    client: self.client.clone(),
                    admin: self.admin.clone(),
                };
                tokio::spawn(async move {
                    let _ = tx.send(Msg::Inbox(crate::inbox::list(&src, false).await));
                });
            }
            Effect::InboxAct(a) => {
                let src = InboxSource {
                    client: self.client.clone(),
                    admin: self.admin.clone(),
                };
                tokio::spawn(async move {
                    let s = match crate::inbox::act(&src, &a).await {
                        Ok(_) => format!(
                            "{} {}: done",
                            a.id,
                            format!("{:?}", a.kind).to_ascii_lowercase()
                        ),
                        Err(e) => format!("{}: {e}", a.id),
                    };
                    let _ = tx.send(Msg::Status(s));
                    let _ = tx.send(Msg::Inbox(crate::inbox::list(&src, false).await));
                });
            }
        }
        true
    }
}

/// Applies a table tab's statements in one transaction: a statement that
/// names a row and changes none is a conflict, which rolls everything back
/// and reads those rows as they are now.
async fn table_apply(
    c: &DataClient,
    db: &str,
    stmts: &[crate::tui::table::ApplyStmt],
    caps: Caps,
) -> Result<Vec<crate::tui::table::Conflict>, crate::error::ShellError> {
    let h = c.begin(Some(db), false).await?;
    let mut stale = Vec::new();
    for s in stmts {
        let mut out = Collect::default();
        if let Err(e) = c
            .query_in(Some(db), "sql", &s.sql, Some(&h), caps, &mut out)
            .await
        {
            let _ = c.end(&h, false).await;
            return Err(e);
        }
        let affected = ResultSet::from_events(&out.0)
            .last()
            .and_then(|r| r.end.as_ref())
            .map_or(0, |e| e.affected);
        if affected == 0
            && let Some(q) = &s.reread
        {
            stale.push((s.row, q.clone()));
        }
    }
    if stale.is_empty() {
        c.end(&h, true).await?;
        return Ok(Vec::new());
    }
    c.end(&h, false).await?;
    let mut out = Vec::new();
    for (row, q) in stale {
        let mut s = Collect::default();
        let now = match c.query_in(Some(db), "sql", &q, None, caps, &mut s).await {
            Ok(()) => ResultSet::from_events(&s.0)
                .into_iter()
                .find(|r| !r.columns.is_empty())
                .and_then(|r| r.rows.into_iter().next()),
            Err(_) => None,
        };
        out.push((row, now));
    }
    Ok(out)
}

/// Puts `text` on the clipboard with OSC 52, which most terminals (and
/// tmux with `set-clipboard on`) pass to the system clipboard.
fn copy_osc52(text: &str) {
    use base64::Engine as _;
    use std::io::Write as _;
    let b = base64::engine::general_purpose::STANDARD.encode(text);
    let mut out = std::io::stdout();
    let _ = write!(out, "\x1b]52;c;{b}\x07");
    let _ = out.flush();
}

/// Writes a tab's shown result to `./sprig-<time>.<ext>`.
fn export(app: &App, tab: TabId, format: crate::output::Format) -> String {
    let Some(t) = app.tabs.iter().find(|t| t.id == tab) else {
        return String::new();
    };
    let Some(set) = t.results.set() else {
        return "nothing to export".to_owned();
    };
    let ext = match format {
        crate::output::Format::Csv => "csv",
        crate::output::Format::Json => "json",
        crate::output::Format::Ndjson => "ndjson",
        crate::output::Format::Tsv => "tsv",
        crate::output::Format::Table => "txt",
    };
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let path = PathBuf::from(format!("sprig-{secs}.{ext}"));
    let file = match std::fs::File::create(&path) {
        Ok(f) => f,
        Err(e) => return format!("{}: {e}", path.display()),
    };
    let mut w = Writer::new(std::io::BufWriter::new(file), format);
    let mut evs = Vec::new();
    if !set.columns.is_empty() {
        evs.push(ResultEvent::Header(set.columns.clone()));
        evs.push(ResultEvent::Rows(set.rows.clone()));
    }
    evs.extend(set.docs.iter().cloned().map(ResultEvent::Doc));
    evs.push(ResultEvent::End(set.end.clone().unwrap_or_default()));
    for ev in evs {
        if let Err(e) = w.event(ev) {
            return format!("{}: {e}", path.display());
        }
    }
    let _ = w.finish();
    let n = set.rows.len().max(set.docs.len());
    format!("wrote {n} rows to {}", path.display())
}

fn restore() {
    let _ = disable_raw_mode();
    let _ = execute!(
        std::io::stdout(),
        DisableBracketedPaste,
        LeaveAlternateScreen
    );
    let _ = std::io::stdout().flush();
}

/// Runs the TUI until the user quits.
pub async fn run(ctx: Ctx, protocol: Protocol, caps: Caps, lang: Lang, admin: bool) -> ExitCode {
    open(
        ctx,
        protocol,
        caps,
        lang,
        if admin { Some(Fixed::Admin) } else { None },
    )
    .await
}

/// `sprig top`: the TUI on the Dashboard tab.
pub async fn run_dashboard(ctx: Ctx) -> ExitCode {
    open(
        ctx,
        Protocol::Http,
        Caps {
            max_rows: 10_000,
            ..Caps::default()
        },
        Lang::Sql,
        Some(Fixed::Dash),
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn open(
    ctx: Ctx,
    protocol: Protocol,
    caps: Caps,
    lang: Lang,
    start: Option<Fixed>,
) -> ExitCode {
    // Sign in before the screen changes, so a password prompt works.
    let runner = match ctx.connect(protocol, caps).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sprig: {e}");
            return ExitCode::from(e.class().code());
        }
    };
    let target = if protocol == Protocol::Http {
        None
    } else {
        ctx.native_target(protocol).ok()
    };
    let admin = AdminApi::of(&ctx.profile).ok().flatten();
    let history = History::load(
        Some(&ctx.dir),
        &ctx.name,
        ctx.profile.history_size.unwrap_or(10_000),
    );
    let refresh = Duration::from_secs(ctx.profile.refresh_seconds());
    let first = DataTab::new(1, lang, runner.describe(), runner.database.clone());
    let mut app = App::new(
        &ctx.name,
        first,
        history,
        ctx.profile.confirm.unwrap_or_default(),
        admin.is_some(),
    );
    app.saved = crate::saved::Saved::load(Some(&ctx.dir), &ctx.name);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = runner.client.clone();
    let mut rt = Rt {
        ctx: Arc::new(ctx),
        protocol,
        caps,
        client,
        target,
        runners: HashMap::from([(1, Arc::new(Mutex::new(runner)))]),
        tasks: HashMap::new(),
        tx,
        admin,
        rates: Rates::default(),
        last_auth: Instant::now(),
    };
    // Keys come from a thread reading the terminal.
    let (ktx, mut krx) = mpsc::unbounded_channel::<Event>();
    std::thread::spawn(move || {
        while let Ok(ev) = crossterm::event::read() {
            if ktx.send(ev).is_err() {
                break;
            }
        }
    });
    if enable_raw_mode().is_err() {
        eprintln!("sprig: this terminal doesn't support the TUI; use --plain");
        return ExitCode::from(2);
    }
    let _ = execute!(
        std::io::stdout(),
        EnterAlternateScreen,
        EnableBracketedPaste
    );
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        prev(info);
    }));
    let mut term = match Terminal::new(CrosstermBackend::new(std::io::stdout())) {
        Ok(t) => t,
        Err(e) => {
            restore();
            eprintln!("sprig: {e}");
            return ExitCode::from(2);
        }
    };
    let mut fx = vec![
        Effect::LoadCatalog,
        Effect::InboxRefresh,
        Effect::LoadLibrary,
    ];
    if let Some(f) = start {
        fx.extend(app.show(f));
    }
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut inbox_tick = tokio::time::interval(Duration::from_secs(60));
    inbox_tick.tick().await;
    'outer: loop {
        for e in fx.drain(..) {
            if !rt.apply(&mut app, e) {
                break 'outer;
            }
        }
        if app.quit {
            break;
        }
        let _ = term.draw(|f| app.draw(f));
        tokio::select! {
            ev = krx.recv() => match ev {
                Some(Event::Key(k)) if k.kind != KeyEventKind::Release => fx = app.on_key(k),
                Some(Event::Paste(s)) => {
                    if let Some(t) = app.tabs.get_mut(app.screen) {
                        t.editor.insert_str(&s);
                    } else if app.fixed() == Some(Fixed::Admin) {
                        app.admin.input.insert_str(&s.replace('\n', " "));
                    }
                }
                Some(_) => {}
                None => break,
            },
            m = rx.recv() => if let Some(mut m) = m {
                if let Msg::Dash(p) = &mut m
                    && p.view == "fingerprints"
                {
                    rt.rates.apply(p, "runs");
                }
                fx = app.on_msg(m);
            },
            _ = tick.tick() => {
                rt.keep_signed_in().await;
                fx = app.tick(refresh);
            },
            _ = inbox_tick.tick() => fx = vec![Effect::InboxRefresh],
        }
    }
    for (h, _, _) in rt.tasks.values() {
        h.abort();
    }
    restore();
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests;
