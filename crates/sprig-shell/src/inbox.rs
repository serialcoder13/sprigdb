//! The approvals inbox.
//!
//! Operators (a profile with `admin_url`) use the admin API's `Recommendations`
//! service: `GET /admin/v1/recommendations?open=true` and `POST
//! /admin/v1/recommendations/{id}:{action}`. Tenant users use its SQL:
//! `SHOW SPRIG RECOMMENDATIONS` and `SPRIG APPROVE | IGNORE | SNOOZE
//! RECOMMENDATION`. Where the deployment doesn't serve them yet (404,
//! 1064), the inbox says so instead of failing.

use std::process::ExitCode;

use clap::{Arg, ArgAction, ArgMatches, Command};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph, Row, Table, Wrap};
use serde_json::{Map, Value, json};

use crate::admin::AdminApi;
use crate::api::{Caps, DataClient, encode};
use crate::cli::Ctx;
use crate::error::{Result, ShellCode, ShellError};
use crate::model::{Collect, ResultSet};
use crate::msg::lit;

/// What to do with a recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActKind {
    /// Approve now.
    Approve,
    /// Approve in a window (`START/END`, RFC 3339).
    Window,
    /// Ignore for N days, or `forever`.
    Ignore,
    /// Snooze for N hours.
    Snooze,
    /// Comment.
    Comment,
}

/// An action on one recommendation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxAction {
    /// The recommendation's id (`t42.17`, `dc.mum1.3`, `g.5`, or the bare
    /// number over SQL).
    pub id: String,
    /// What.
    pub kind: ActKind,
    /// Days, hours, a window or a comment.
    pub input: Option<String>,
    /// A reason.
    pub reason: Option<String>,
}

impl InboxAction {
    /// The action with the user's typed text.
    #[must_use]
    pub fn with_input(mut self, text: &str) -> Self {
        self.input = Some(text.trim().to_owned());
        self
    }

    fn action_name(&self) -> &'static str {
        match self.kind {
            ActKind::Approve | ActKind::Window => "approve",
            ActKind::Ignore => "ignore",
            ActKind::Snooze => "snooze",
            ActKind::Comment => "comment",
        }
    }

    /// The admin API's action body.
    ///
    /// # Errors
    /// 39985 for input that doesn't parse.
    pub fn body(&self) -> Result<Value> {
        let bad = |m: &str| ShellError::shell(ShellCode::Parse, m.to_owned());
        let input = self.input.clone().unwrap_or_default();
        let mut b = json!({ "action": self.action_name() });
        if let Some(r) = &self.reason {
            b["reason"] = Value::String(r.clone());
        }
        match self.kind {
            ActKind::Approve => {}
            ActKind::Window => {
                let (s, e) = input.split_once('/').ok_or_else(|| {
                    bad("a window is START/END, such as 2026-11-11T20:00:00Z/2026-11-12T02:00:00Z")
                })?;
                b["window"] = json!({ "start": s.trim(), "end": e.trim() });
            }
            ActKind::Ignore => {
                if input.eq_ignore_ascii_case("forever") {
                    b["forever"] = Value::Bool(true);
                } else {
                    let d: u32 = input
                        .parse()
                        .map_err(|_| bad("ignore takes a number of days or 'forever'"))?;
                    b["for_days"] = json!(d);
                }
                if b.get("reason").is_none() {
                    b["reason"] = Value::String("ignored from sprig".to_owned());
                }
            }
            ActKind::Snooze => {
                let h: u32 = input
                    .parse()
                    .map_err(|_| bad("snooze takes a number of hours"))?;
                b["hours"] = json!(h);
            }
            ActKind::Comment => {
                if input.is_empty() {
                    return Err(bad("an empty comment"));
                }
                b["text"] = Value::String(input);
            }
        }
        Ok(b)
    }

    /// The SQL form, for tenant users.
    ///
    /// # Errors
    /// 39983 for windows and comments, which SQL doesn't carry; 39985.
    pub fn sql(&self) -> Result<String> {
        let n = self.id.rsplit('.').next().unwrap_or(&self.id);
        if !n.chars().all(|c| c.is_ascii_digit()) || n.is_empty() {
            return Err(ShellError::shell(
                ShellCode::Parse,
                format!("not a recommendation id: {}", self.id),
            ));
        }
        let reason = self
            .reason
            .as_ref()
            .map(|r| format!(" REASON {}", lit(r)))
            .unwrap_or_default();
        let input = self.input.clone().unwrap_or_default();
        Ok(match self.kind {
            ActKind::Approve => format!("SPRIG APPROVE RECOMMENDATION {n}{reason}"),
            ActKind::Ignore if input.eq_ignore_ascii_case("forever") => {
                format!("SPRIG IGNORE RECOMMENDATION {n} FOREVER{reason}")
            }
            ActKind::Ignore => {
                let d: u32 = input.parse().map_err(|_| {
                    ShellError::shell(
                        ShellCode::Parse,
                        "ignore takes a number of days or 'forever'",
                    )
                })?;
                format!("SPRIG IGNORE RECOMMENDATION {n} FOR {d} DAYS{reason}")
            }
            ActKind::Snooze => {
                let h: u32 = input.parse().map_err(|_| {
                    ShellError::shell(ShellCode::Parse, "snooze takes a number of hours")
                })?;
                format!("SPRIG SNOOZE RECOMMENDATION {n} FOR {h} HOURS")
            }
            ActKind::Window | ActKind::Comment => {
                return Err(ShellError::shell(
                    ShellCode::LanguageUnsupported,
                    "windows and comments need the Admin API (SQL has no statement for them)",
                ));
            }
        })
    }
}

/// Where the inbox reads and acts.
#[derive(Clone)]
pub struct InboxSource {
    /// The data API (SQL), as the user.
    pub client: DataClient,
    /// The Admin API, for operators.
    pub admin: Option<AdminApi>,
}

/// Whether an error means "this deployment doesn't serve it yet".
#[must_use]
pub fn not_served(e: &ShellError) -> bool {
    e.http_status == 404 || matches!(e.code, 1064 | 1149)
}

/// Lists open recommendations: `{"items": [...], "source": "admin" | "sql"}`.
///
/// # Errors
/// The API's error.
pub async fn list(src: &InboxSource, all: bool) -> Result<Value> {
    if let Some(a) = &src.admin {
        let path = if all {
            "/admin/v1/recommendations"
        } else {
            "/admin/v1/recommendations?open=true"
        };
        let v = a.get(path).await?;
        let items = match &v {
            Value::Array(a) => a.clone(),
            Value::Object(m) => m
                .values()
                .find_map(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        return Ok(json!({ "items": items, "source": "admin" }));
    }
    let sql = if all {
        "SHOW SPRIG RECOMMENDATIONS ALL"
    } else {
        "SHOW SPRIG RECOMMENDATIONS"
    };
    let mut c = Collect::default();
    src.client
        .sql_query(None, sql, None, Caps::default(), &mut c)
        .await?;
    let items: Vec<Value> = ResultSet::from_events(&c.0)
        .into_iter()
        .flat_map(|s| {
            let cols = s.columns.clone();
            s.rows.into_iter().map(move |r| {
                let m: Map<String, Value> = cols
                    .iter()
                    .zip(r)
                    .map(|(c, v)| (c.name.clone(), v.map_or(Value::Null, Value::String)))
                    .collect();
                Value::Object(m)
            })
        })
        .collect();
    Ok(json!({ "items": items, "source": "sql" }))
}

/// One recommendation with evidence, history and comments (Admin API).
///
/// # Errors
/// The API's error; 39983 without an Admin API.
pub async fn show(src: &InboxSource, id: &str) -> Result<Value> {
    match &src.admin {
        Some(a) => {
            a.get(&format!("/admin/v1/recommendations/{}", encode(id)))
                .await
        }
        None => Err(ShellError::shell(
            ShellCode::LanguageUnsupported,
            "an item's evidence and history need the Admin API; SHOW SPRIG RECOMMENDATIONS lists the summary",
        )),
    }
}

/// Acts on a recommendation.
///
/// # Errors
/// The API's error (39973, 39974, 39976 ...).
pub async fn act(src: &InboxSource, a: &InboxAction) -> Result<Value> {
    if let Some(admin) = &src.admin {
        let path = format!(
            "/admin/v1/recommendations/{}:{}",
            encode(&a.id),
            a.action_name()
        );
        return admin.post(&path, &a.body()?).await;
    }
    let sql = a.sql()?;
    let mut c = Collect::default();
    src.client
        .sql_query(None, &sql, None, Caps::default(), &mut c)
        .await?;
    Ok(json!({ "ok": true, "sql": sql }))
}

/// What a key did in the inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyOutcome {
    /// Nothing for the runtime.
    None,
    /// Reload.
    Refresh,
    /// Act now.
    Act(InboxAction),
    /// Ask yes or no first.
    Confirm(String, InboxAction),
    /// Ask for text first.
    Ask(String, InboxAction),
}

/// The inbox's state.
#[derive(Debug, Default)]
pub struct Inbox {
    /// Open items as JSON objects.
    pub items: Vec<Value>,
    /// Where they came from.
    pub source: String,
    /// Selected row.
    pub row: usize,
    /// A message instead of a list.
    pub note: Option<String>,
    /// Loaded at least once.
    pub loaded: bool,
}

fn field(v: &Value, keys: &[&str]) -> String {
    for k in keys {
        let mut cur = v;
        let mut ok = true;
        for part in k.split('.') {
            if let Some(x) = cur.get(part) {
                cur = x;
            } else {
                ok = false;
                break;
            }
        }
        if ok {
            return match cur {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            };
        }
    }
    String::new()
}

impl Inbox {
    /// The count for the badge, once loaded.
    #[must_use]
    pub fn count(&self) -> Option<usize> {
        self.loaded.then_some(self.items.len())
    }

    /// Takes a list result.
    pub fn set(&mut self, r: Result<Value>) {
        match r {
            Ok(v) => {
                self.items = v
                    .get("items")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                self.source = field(&v, &["source"]);
                self.note = None;
                self.loaded = true;
                self.row = self.row.min(self.items.len().saturating_sub(1));
            }
            Err(e) if not_served(&e) => {
                self.items.clear();
                self.loaded = false;
                self.note = Some(
                    "This deployment doesn't serve the Recommendations API yet. \
                     The inbox fills in once it does."
                        .to_owned(),
                );
            }
            Err(e) => self.note = Some(e.to_string()),
        }
    }

    fn selected_id(&self) -> Option<String> {
        self.items
            .get(self.row)
            .map(|v| field(v, &["id", "rec_id"]))
            .filter(|s| !s.is_empty())
    }

    /// Handles a key.
    pub fn on_key(&mut self, k: KeyEvent) -> KeyOutcome {
        let act = |kind| InboxAction {
            id: String::new(),
            kind,
            input: None,
            reason: None,
        };
        match k.code {
            KeyCode::Up => {
                self.row = self.row.saturating_sub(1);
                KeyOutcome::None
            }
            KeyCode::Down => {
                self.row = (self.row + 1).min(self.items.len().saturating_sub(1));
                KeyOutcome::None
            }
            KeyCode::Char('r') => KeyOutcome::Refresh,
            KeyCode::Char(c @ ('a' | 'w' | 'i' | 's' | 'c')) => {
                let Some(id) = self.selected_id() else {
                    return KeyOutcome::None;
                };
                let summary = self
                    .items
                    .get(self.row)
                    .map(|v| field(v, &["summary"]))
                    .unwrap_or_default();
                match c {
                    'a' => KeyOutcome::Confirm(
                        format!("Approve {id}?\n{summary}"),
                        InboxAction {
                            id,
                            ..act(ActKind::Approve)
                        },
                    ),
                    'w' => KeyOutcome::Ask(
                        format!("Approve {id} in a window: START/END (RFC 3339)"),
                        InboxAction {
                            id,
                            ..act(ActKind::Window)
                        },
                    ),
                    'i' => KeyOutcome::Ask(
                        format!("Ignore {id} for how many days? (or 'forever')"),
                        InboxAction {
                            id,
                            ..act(ActKind::Ignore)
                        },
                    ),
                    's' => KeyOutcome::Ask(
                        format!("Snooze {id} for how many hours?"),
                        InboxAction {
                            id,
                            ..act(ActKind::Snooze)
                        },
                    ),
                    _ => KeyOutcome::Ask(
                        format!("Comment on {id}"),
                        InboxAction {
                            id,
                            ..act(ActKind::Comment)
                        },
                    ),
                }
            }
            _ => KeyOutcome::None,
        }
    }

    /// Draws the inbox: the list, then the selected item's action and
    /// evidence.
    pub fn draw(&self, f: &mut Frame<'_>, area: Rect) {
        let title = format!(
            " Approvals{} · a approve · w window · i ignore · s snooze · c comment · r refresh ",
            if self.source.is_empty() {
                String::new()
            } else {
                format!(" ({})", self.source)
            }
        );
        let block = Block::bordered().title(title);
        let inner = block.inner(area);
        f.render_widget(block, area);
        if let Some(n) = &self.note {
            f.render_widget(
                Paragraph::new(Line::styled(n.clone(), crate::tui::theme::warn()))
                    .wrap(Wrap { trim: true }),
                inner,
            );
            return;
        }
        if self.items.is_empty() {
            let msg = if self.loaded {
                "Nothing waits for you."
            } else {
                "loading..."
            };
            f.render_widget(Paragraph::new(msg), inner);
            return;
        }
        let [list, detail] =
            Layout::vertical([Constraint::Percentage(55), Constraint::Min(4)]).areas(inner);
        let cols = ["id", "kind", "level", "state", "target", "age", "summary"];
        let rows = self.items.iter().enumerate().map(|(i, v)| {
            let cells = vec![
                field(v, &["id", "rec_id"]),
                field(v, &["kind"]),
                field(v, &["level"]),
                field(v, &["state"]),
                field(v, &["target"]),
                field(v, &["age_hours", "age"]),
                field(v, &["summary"]),
            ];
            let r = Row::new(cells);
            if i == self.row {
                r.style(crate::tui::theme::selected())
            } else {
                r
            }
        });
        let widths = [
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(22),
            Constraint::Length(5),
            Constraint::Min(10),
        ];
        f.render_widget(
            Table::new(rows, widths)
                .header(Row::new(cols).style(Style::new().add_modifier(Modifier::BOLD)))
                .column_spacing(1),
            list,
        );
        let Some(v) = self.items.get(self.row) else {
            return;
        };
        let mut lines = vec![Line::styled(
            format!("Action: {}", field(v, &["action.text", "action"])),
            crate::tui::theme::selected(),
        )];
        let auto = field(v, &["auto_apply_at"]);
        if !auto.is_empty() {
            lines.push(Line::from(format!("Applies by itself at {auto}")));
        }
        if let Some(ev) = v.get("evidence") {
            for l in serde_json::to_string_pretty(ev).unwrap_or_default().lines() {
                lines.push(Line::from(l.to_owned()));
            }
        }
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(" Evidence ")),
            detail,
        );
    }
}

/// `sprig approvals`.
#[must_use]
pub fn command() -> Command {
    let id = || {
        Arg::new("id")
            .required(true)
            .help("Recommendation id (t42.17, dc.mum1.3, g.5, or a number)")
    };
    let reason = || Arg::new("reason").long("reason");
    Command::new("approvals")
        .about("The approvals inbox: recommendations waiting for a decision")
        .subcommand_required(true)
        .subcommand(
            Command::new("list")
                .about("Open recommendations you may act on")
                .arg(Arg::new("all").long("all").action(ArgAction::SetTrue)),
        )
        .subcommand(
            Command::new("show")
                .about("One item with evidence and history")
                .arg(id()),
        )
        .subcommand(
            Command::new("approve").arg(id()).arg(reason()).arg(
                Arg::new("window")
                    .long("window")
                    .help("START/END in RFC 3339"),
            ),
        )
        .subcommand(
            Command::new("ignore")
                .arg(id())
                .arg(reason())
                .arg(Arg::new("days").long("days"))
                .arg(
                    Arg::new("forever")
                        .long("forever")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("snooze")
                .arg(id())
                .arg(Arg::new("hours").long("hours").required(true)),
        )
        .subcommand(
            Command::new("comment")
                .arg(id())
                .arg(Arg::new("text").required(true)),
        )
}

/// The inbox's source for a profile.
///
/// # Errors
/// Sign-in errors.
pub async fn source(ctx: &Ctx) -> Result<InboxSource> {
    let admin = AdminApi::of(&ctx.profile)?;
    let runner = ctx
        .connect(crate::native::Protocol::Http, Caps::default())
        .await;
    let client = match (runner, &admin) {
        (Ok(r), _) => r.client,
        // Operators may have no data credential: SQL isn't needed then.
        (Err(_), Some(_)) => DataClient::new(
            crate::session::http_of(&ctx.profile)?,
            crate::api::Auth::None,
        ),
        (Err(e), None) => return Err(e),
    };
    Ok(InboxSource { client, admin })
}

/// Runs `sprig approvals ...`.
pub async fn cli(ctx: &Ctx, top: &ArgMatches, m: &ArgMatches) -> ExitCode {
    let src = match source(ctx).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sprig: {e}");
            return ExitCode::from(e.class().code());
        }
    };
    let json_out = top
        .get_one::<String>("format")
        .is_some_and(|f| f == "json" || f == "ndjson");
    let get = |s: &ArgMatches, k: &str| s.get_one::<String>(k).cloned();
    let r = match m.subcommand() {
        Some(("list", s)) => list(&src, s.get_flag("all")).await.map(|v| {
            if json_out {
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            } else {
                let items = v
                    .get("items")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                println!(
                    "{:<12} {:<14} {:<9} {:<9} {:<24} SUMMARY",
                    "ID", "KIND", "LEVEL", "STATE", "TARGET"
                );
                for i in &items {
                    println!(
                        "{:<12} {:<14} {:<9} {:<9} {:<24} {}",
                        field(i, &["id", "rec_id"]),
                        field(i, &["kind"]),
                        field(i, &["level"]),
                        field(i, &["state"]),
                        crate::tui::clip(&field(i, &["target"]), 24),
                        field(i, &["summary"])
                    );
                }
                eprintln!("{} open", items.len());
            }
        }),
        Some(("show", s)) => show(&src, &get(s, "id").unwrap_or_default())
            .await
            .map(|v| println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default())),
        Some((name, s)) => {
            let id = get(s, "id").unwrap_or_default();
            let (kind, input) = match name {
                "approve" => match get(s, "window") {
                    Some(w) => (ActKind::Window, Some(w)),
                    None => (ActKind::Approve, None),
                },
                "ignore" => (
                    ActKind::Ignore,
                    Some(if s.get_flag("forever") {
                        "forever".to_owned()
                    } else {
                        get(s, "days").unwrap_or_default()
                    }),
                ),
                "snooze" => (ActKind::Snooze, get(s, "hours")),
                _ => (ActKind::Comment, get(s, "text")),
            };
            let a = InboxAction {
                id,
                kind,
                input,
                reason: get(s, "reason"),
            };
            act(&src, &a).await.map(|v| {
                if json_out {
                    println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                } else {
                    eprintln!("{name}: done");
                }
            })
        }
        None => Ok(()),
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if not_served(&e) {
                eprintln!("sprig: this deployment doesn't serve the Recommendations API yet ({e})");
            } else {
                eprintln!("sprig: {e}");
            }
            ExitCode::from(e.class().code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(kind: ActKind, input: Option<&str>) -> InboxAction {
        InboxAction {
            id: "t42.17".into(),
            kind,
            input: input.map(str::to_owned),
            reason: None,
        }
    }

    #[test]
    fn bodies_follow_chapter_52() {
        assert_eq!(
            a(ActKind::Approve, None).body().expect("b"),
            json!({"action": "approve"})
        );
        assert_eq!(
            a(
                ActKind::Window,
                Some("2026-11-11T20:00:00Z/2026-11-12T02:00:00Z")
            )
            .body()
            .expect("b"),
            json!({"action": "approve", "window": {"start": "2026-11-11T20:00:00Z", "end": "2026-11-12T02:00:00Z"}})
        );
        assert_eq!(
            a(ActKind::Ignore, Some("30")).body().expect("b"),
            json!({"action": "ignore", "for_days": 30, "reason": "ignored from sprig"})
        );
        assert_eq!(
            a(ActKind::Ignore, Some("forever")).body().expect("b")["forever"],
            json!(true)
        );
        assert_eq!(
            a(ActKind::Snooze, Some("48")).body().expect("b"),
            json!({"action": "snooze", "hours": 48})
        );
        assert!(a(ActKind::Snooze, Some("soon")).body().is_err());
        assert_eq!(
            a(ActKind::Comment, Some("ok")).body().expect("b"),
            json!({"action": "comment", "text": "ok"})
        );
    }

    #[test]
    fn sql_forms() {
        assert_eq!(
            a(ActKind::Approve, None).sql().expect("s"),
            "SPRIG APPROVE RECOMMENDATION 17"
        );
        assert_eq!(
            a(ActKind::Ignore, Some("30")).sql().expect("s"),
            "SPRIG IGNORE RECOMMENDATION 17 FOR 30 DAYS"
        );
        assert_eq!(
            a(ActKind::Ignore, Some("forever")).sql().expect("s"),
            "SPRIG IGNORE RECOMMENDATION 17 FOREVER"
        );
        assert_eq!(
            a(ActKind::Snooze, Some("48")).sql().expect("s"),
            "SPRIG SNOOZE RECOMMENDATION 17 FOR 48 HOURS"
        );
        assert_eq!(
            a(ActKind::Comment, Some("x"))
                .sql()
                .expect_err("no sql")
                .code,
            39983
        );
    }

    #[test]
    fn not_served_yet() {
        let mut i = Inbox::default();
        let mut e = ShellError::server(0, "", "not found");
        e.http_status = 404;
        i.set(Err(e));
        assert!(
            i.note
                .as_deref()
                .is_some_and(|n| n.contains("doesn't serve"))
        );
        assert_eq!(i.count(), None);
        i.set(Ok(json!({"items": [{"id": "t1.2", "summary": "index on orders(status)"}], "source": "admin"})));
        assert_eq!(i.count(), Some(1));
        assert_eq!(
            i.on_key(KeyEvent::from(KeyCode::Char('s'))),
            KeyOutcome::Ask(
                "Snooze t1.2 for how many hours?".into(),
                InboxAction {
                    id: "t1.2".into(),
                    kind: ActKind::Snooze,
                    input: None,
                    reason: None
                }
            )
        );
    }
}
