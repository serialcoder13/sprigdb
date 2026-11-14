//! Screen snapshots with ratatui's `TestBackend`, and key behaviour. Set `SPRIG_UPDATE_GOLDEN=1` to rewrite the screens
//! after a deliberate change; they live in `src/tui/golden/`.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serde_json::json;

use super::app::{App, DataTab, Dialog, Effect, Fixed, Focus, Msg};
use crate::dash::Panel;
use crate::history::History;
use crate::lang::Lang;
use crate::model::{Column, EndInfo, ResultEvent, ResultSet};
use crate::profile::Confirm;

fn app() -> App {
    let mut t = DataTab::new(
        1,
        Lang::Sql,
        "127.0.0.1:8080 db shop".into(),
        Some("shop".into()),
    );
    t.editor
        .insert_str("SELECT order_id, status\nFROM orders\nWHERE status = 'paid';");
    let mut a = App::new(
        "dev",
        t,
        History::load(None, "dev", 100),
        Confirm::Destructive,
        false,
    );
    a.on_msg(Msg::Catalog(Ok(json!({"databases": ["shop", "hr"]}))));
    a.on_msg(Msg::Db(
        "shop".into(),
        Ok(json!({"db": "shop", "objects": [
            {"name": "orders", "kind": "table", "rows": 3},
            {"name": "order_items", "kind": "table"},
            {"name": "v_paid", "kind": "view"}],
          "columns": {"orders": [{"name": "order_id", "type": "bigint"}, {"name": "status", "type": "varchar(16)"}]}})),
        vec!["order_events".into()],
        vec![],
    ));
    a
}

fn col(n: &str, t: &str) -> Column {
    Column {
        name: n.into(),
        type_name: t.into(),
        binary: false,
    }
}

fn with_rows(a: &mut App) {
    a.on_msg(Msg::Events(
        1,
        vec![
            ResultEvent::Header(vec![
                col("order_id", "BIGINT"),
                col("status", "VARCHAR"),
                col("note", "TEXT"),
            ]),
            ResultEvent::Rows(vec![
                vec![Some("7001".into()), Some("paid".into()), None],
                vec![
                    Some("7002".into()),
                    Some("paid".into()),
                    Some("gift, wrap it".into()),
                ],
                vec![
                    Some("17".into()),
                    Some("paid".into()),
                    Some(
                        "a very long note that goes on and on past the column cap of forty".into(),
                    ),
                ],
            ]),
            ResultEvent::End(EndInfo::default()),
        ],
    ));
    a.on_msg(Msg::Done {
        tab: 1,
        error: None,
        elapsed_ms: 12,
        describe: "127.0.0.1:8080 db shop".into(),
        database: Some("shop".into()),
        in_txn: false,
    });
}

fn screen(a: &mut App, w: u16, h: u16) -> String {
    let mut term = Terminal::new(TestBackend::new(w, h)).expect("terminal");
    term.draw(|f| a.draw(f)).expect("draw");
    let buf = term.backend().buffer().clone();
    let mut out = String::new();
    for y in 0..h {
        let mut line = String::new();
        for x in 0..w {
            line.push_str(buf[(x, y)].symbol());
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

fn golden(name: &str, got: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/tui/golden")
        .join(name);
    if std::env::var_os("SPRIG_UPDATE_GOLDEN").is_some() {
        let _ = std::fs::create_dir_all(path.parent().expect("dir"));
        std::fs::write(&path, got).expect("write golden");
        return;
    }
    let want = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        got,
        want,
        "{} differs; SPRIG_UPDATE_GOLDEN=1 rewrites it",
        path.display()
    );
}

fn key(c: KeyCode) -> KeyEvent {
    KeyEvent::from(c)
}

fn ctrl(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

#[test]
fn data_tab_with_grid() {
    let mut a = app();
    with_rows(&mut a);
    golden("data_grid.txt", &screen(&mut a, 110, 26));
}

#[test]
fn completion_menu() {
    let mut a = app();
    a.tabs[0].editor = super::editor::Editor::with_text("SELECT * FROM ord");
    let fx = a.on_key(key(KeyCode::Tab));
    assert!(fx.is_empty());
    assert_eq!(a.completion.as_ref().map(|c| c.items.len()), Some(3));
    golden("completion.txt", &screen(&mut a, 110, 20));
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Enter));
    assert_eq!(a.tabs[0].editor.text(), "SELECT * FROM order_items");
}

#[test]
fn json_tree_and_graph_views() {
    let mut a = app();
    a.tabs[0].lang = Lang::Cypher;
    a.on_msg(Msg::Events(1, vec![
        // Keys in sorted order, so the screens are the same with or without
        // serde_json's preserve_order (another crate may turn it on).
        ResultEvent::Doc(json!({"_id": {"$oid": "65a1f0"}, "addr": {"city": "Pune"}, "qty": {"$numberLong": "3"}, "tags": ["a", "b"]})),
        ResultEvent::Doc(json!({"p": {"$path": [
            {"$node": {"id": 1, "labels": ["Person"], "props": {"name": "Asha"}}},
            {"$rel": {"end": 2, "id": 9, "props": {}, "start": 1, "type": "KNOWS"}},
            {"$node": {"id": 2, "labels": ["Person"], "props": {"name": "Ravi"}}}]}})),
        ResultEvent::End(EndInfo::default()),
    ]));
    a.on_msg(Msg::Done {
        tab: 1,
        error: None,
        elapsed_ms: 3,
        describe: "bolt://127.0.0.1:7687".into(),
        database: None,
        in_txn: false,
    });
    a.on_key(key(KeyCode::F(3)));
    golden("json_tree.txt", &screen(&mut a, 100, 30));
    a.focus = Focus::Results;
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Enter));
    assert!(a.tabs[0].results.folded.contains("0.addr"), "folded addr");
    golden("json_folded.txt", &screen(&mut a, 100, 16));
    a.on_key(key(KeyCode::F(3)));
    golden("graph.txt", &screen(&mut a, 100, 24));
}

#[test]
fn destructive_statement_asks_first() {
    let mut a = app();
    a.tabs[0].editor = super::editor::Editor::with_text("DROP TABLE orders;");
    let fx = a.on_key(key(KeyCode::Enter));
    assert!(fx.is_empty());
    assert!(matches!(a.dialog, Some(Dialog::Confirm(..))));
    golden("confirm.txt", &screen(&mut a, 100, 24));
    let fx = a.on_key(key(KeyCode::Char('y')));
    assert_eq!(
        fx,
        vec![Effect::Run(1, vec!["DROP TABLE orders".into()], Lang::Sql)]
    );
    assert!(a.tabs[0].running);
    assert_eq!(a.on_key(ctrl('c')), vec![Effect::Cancel(1)]);
}

#[test]
fn enter_runs_complete_statements_and_keeps_history() {
    let mut a = app();
    a.tabs[0].editor = super::editor::Editor::with_text("SELECT 1; SELECT 2;");
    let fx = a.on_key(key(KeyCode::Enter));
    assert_eq!(
        fx,
        vec![Effect::Run(
            1,
            vec!["SELECT 1".into(), "SELECT 2".into()],
            Lang::Sql
        )]
    );
    assert!(a.tabs[0].editor.is_empty());
    a.on_msg(Msg::Done {
        tab: 1,
        error: None,
        elapsed_ms: 1,
        describe: String::new(),
        database: Some("shop".into()),
        in_txn: false,
    });
    a.tabs[0].editor = super::editor::Editor::with_text("CREATE USER bob IDENTIFIED BY 'pw'");
    let fx = a.on_key(key(KeyCode::F(5)));
    assert_eq!(fx.len(), 1);
    a.on_msg(Msg::Done {
        tab: 1,
        error: None,
        elapsed_ms: 1,
        describe: String::new(),
        database: Some("shop".into()),
        in_txn: false,
    });
    a.tabs[0].editor = super::editor::Editor::default();
    a.on_key(KeyEvent::new(KeyCode::Up, KeyModifiers::CONTROL));
    assert_eq!(
        a.tabs[0].editor.text(),
        "CREATE USER bob IDENTIFIED BY '***'"
    );
    // An unfinished statement gets a new line instead.
    a.tabs[0].editor = super::editor::Editor::with_text("SELECT 'a;");
    assert!(a.on_key(key(KeyCode::Enter)).is_empty());
    assert_eq!(a.tabs[0].editor.line_count(), 2);
}

#[test]
fn error_and_explorer_browse() {
    let mut a = app();
    a.on_msg(Msg::Done {
        tab: 1,
        error: Some(crate::error::ShellError::server(
            1146,
            "",
            "Table 'shop.nope' doesn't exist",
        )),
        elapsed_ms: 4,
        describe: "127.0.0.1:8080 db shop".into(),
        database: Some("shop".into()),
        in_txn: true,
    });
    a.focus = Focus::Explorer;
    golden("error.txt", &screen(&mut a, 100, 20));
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Down));
    // `r` runs a SELECT on the table in the editor; Enter opens its view.
    let fx = a.on_key(key(KeyCode::Char('r')));
    assert_eq!(
        fx,
        vec![Effect::Run(
            1,
            vec!["SELECT * FROM `shop`.`orders` LIMIT 100".into()],
            Lang::Sql
        )]
    );
    a.focus = Focus::Explorer;
    let fx = a.on_key(key(KeyCode::Enter));
    assert_eq!(fx, vec![Effect::TableInfo(2, "shop".into())]);
    assert!(a.tabs[1].table.is_some());
}

#[test]
fn dashboard_sessions() {
    let mut a = app();
    let fx = a.show(Fixed::Dash);
    assert_eq!(fx, vec![Effect::DashRefresh("sessions".into())]);
    a.on_msg(Msg::Dash(Panel {
        view: "sessions".into(),
        columns: vec![
            "Id".into(),
            "User".into(),
            "db".into(),
            "Command".into(),
            "Time".into(),
            "Info".into(),
        ],
        rows: vec![
            vec![
                "41".into(),
                "acme.alice".into(),
                "shop".into(),
                "Query".into(),
                "3".into(),
                "SELECT SLEEP(30)".into(),
            ],
            vec![
                "42".into(),
                "acme.bob".into(),
                "shop".into(),
                "Sleep".into(),
                "120".into(),
                String::new(),
            ],
        ],
        note: None,
    }));
    a.dash.polled = None;
    golden("dash_sessions.txt", &screen(&mut a, 100, 16));
    a.on_key(key(KeyCode::Char('q')));
    assert!(matches!(&a.dialog, Some(Dialog::Confirm(q, _)) if q.contains("41")));
    assert_eq!(
        a.on_key(key(KeyCode::Char('y'))),
        vec![Effect::Kill("41".into(), true)]
    );
    a.on_key(key(KeyCode::Char('3')));
    a.on_msg(Msg::Dash(Panel {
        view: "clusters".into(),
        note: Some("operators only: add admin_url (and admin_token_file) to the profile".into()),
        ..Panel::default()
    }));
    a.dash.polled = None;
    golden("dash_clusters.txt", &screen(&mut a, 100, 16));
}

#[test]
fn admin_tab() {
    let mut a = app();
    a.show(Fixed::Admin);
    for c in "tenant delete acme".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    let fx = a.on_key(key(KeyCode::Enter));
    assert!(fx.is_empty(), "asks first");
    let fx = a.on_key(key(KeyCode::Char('y')));
    assert_eq!(
        fx,
        vec![Effect::Admin(vec![
            "tenant".into(),
            "delete".into(),
            "acme".into(),
            "--yes".into()
        ])]
    );
    a.on_msg(Msg::Admin(
        vec!["tenant".into(), "list".into()],
        Ok(crate::admin::Captured {
            code: 0,
            stdout: "NAME   STATE   CLUSTER\nacme   active  dev-c1\n".into(),
            stderr: String::new(),
        }),
    ));
    golden("admin.txt", &screen(&mut a, 100, 16));
    for c in "tenant list".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    assert_eq!(
        a.on_key(key(KeyCode::Enter)),
        vec![Effect::Admin(vec!["tenant".into(), "list".into()])]
    );
}

#[test]
fn inbox_tab() {
    let mut a = app();
    assert_eq!(a.show(Fixed::Inbox), vec![Effect::InboxRefresh]);
    a.on_msg(Msg::Inbox(Ok(json!({"source": "admin", "items": [
        {"id": "t42.17", "kind": "index_create", "level": "table", "state": "open", "target": "shop.orders",
         "age_hours": 5, "summary": "Index orders(status, created_at)",
         "action": {"text": "CREATE INDEX sprig_auto_1 ON orders (status, created_at)"},
         "evidence": {"fingerprint": "9c41d2e0", "p95_ms": 840, "rows_examined_per_row": 1200}},
        {"id": "dc.mum1.3", "kind": "dc_move", "level": "dc", "state": "open", "target": "acme",
         "age_hours": 30, "summary": "Move acme to pune"}]}))));
    golden("inbox.txt", &screen(&mut a, 110, 24));
    a.on_key(key(KeyCode::Char('i')));
    for c in "30".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    let fx = a.on_key(key(KeyCode::Enter));
    assert!(
        matches!(&fx[..], [Effect::InboxAct(x)] if x.id == "t42.17" && x.input.as_deref() == Some("30"))
    );
}

#[test]
fn small_terminal_and_help() {
    let mut a = app();
    golden("too_small.txt", &screen(&mut a, 50, 10));
    a.on_key(key(KeyCode::F(1)));
    golden("help.txt", &screen(&mut a, 100, 28));
}

#[test]
fn palette_searches_tables_and_opens_their_view() {
    let mut a = app();
    a.on_key(ctrl('k'));
    for c in "ord".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    golden("palette.txt", &screen(&mut a, 100, 28));
    let fx = a.on_key(key(KeyCode::Enter));
    assert!(a.dialog.is_none());
    assert_eq!(fx, vec![Effect::TableInfo(2, "shop".into())]);
    assert_eq!(a.screen, 1);
    assert_eq!(
        a.tabs[1].table.as_ref().map(|v| v.table.as_str()),
        Some("orders")
    );
    a.screen = 0;
    // Commands are there too.
    a.on_key(ctrl('p'));
    for c in "quit".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    a.on_key(key(KeyCode::Enter));
    assert!(a.quit);
}

/// Writes colour HTML snapshots of a few screens to `SPRIG_SCREENSHOT_DIR`, so
/// people can see the theme without a terminal (run with `--ignored` and
/// `COLORTERM=truecolor`).
#[test]
#[ignore = "writes HTML snapshots for review"]
fn html_snapshots() {
    use ratatui::style::{Color, Modifier};
    use std::fmt::Write as _;
    let Ok(dir) = std::env::var("SPRIG_SCREENSHOT_DIR") else {
        return;
    };
    let html = |a: &mut App, w: u16, h: u16| -> String {
        let mut term = Terminal::new(TestBackend::new(w, h)).expect("terminal");
        term.draw(|f| a.draw(f)).expect("draw");
        let buf = term.backend().buffer().clone();
        let css = |c: Color, default: &str| match c {
            Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
            _ => default.to_owned(),
        };
        let mut out = String::from(
            "<!doctype html><meta charset=utf-8><title>sprig TUI</title><style>body{margin:0;background:#17191d}pre{margin:0;padding:12px;font:14px/1.25 'SF Mono',Menlo,monospace;color:#e6e8eb;background:#17191d}</style><pre>",
        );
        for y in 0..h {
            for x in 0..w {
                let c = &buf[(x, y)];
                let (mut fg, mut bg) = (css(c.fg, "#e6e8eb"), css(c.bg, "transparent"));
                if c.modifier.contains(Modifier::REVERSED) {
                    std::mem::swap(&mut fg, &mut bg);
                }
                let weight = if c.modifier.contains(Modifier::BOLD) {
                    "font-weight:700;"
                } else {
                    ""
                };
                let style = if c.modifier.contains(Modifier::ITALIC) {
                    "font-style:italic;"
                } else {
                    ""
                };
                let sym = c
                    .symbol()
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;");
                let _ = write!(
                    out,
                    "<span style=\"color:{fg};background:{bg};{weight}{style}\">{sym}</span>"
                );
            }
            out.push('\n');
        }
        out.push_str("</pre>");
        out
    };
    let mut a = app();
    std::fs::write(format!("{dir}/welcome.html"), html(&mut a, 110, 30)).expect("write");
    with_rows(&mut a);
    std::fs::write(format!("{dir}/grid.html"), html(&mut a, 110, 30)).expect("write");
    a.on_key(ctrl('k'));
    for c in "ord".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    std::fs::write(format!("{dir}/palette.html"), html(&mut a, 110, 30)).expect("write");
}

#[test]
fn a_question_mark_in_the_palette_writes_an_ask() {
    let mut a = app();
    a.tabs[0].editor.set_text("");
    a.on_key(ctrl('k'));
    for c in "?top 5 customers".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    let shown = a.palette_shown("?top 5 customers", 14);
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].label, "Ask: top 5 customers");
    let fx = a.on_key(key(KeyCode::Enter));
    // Written into the editor, not run.
    assert!(fx.is_empty());
    assert_eq!(a.tabs[0].editor.text(), "ASK 'top 5 customers';");
}

#[test]
fn ctrl_s_saves_and_the_palette_finds_saved_queries_and_history() {
    let mut a = app();
    a.on_key(ctrl('s'));
    for c in "paid orders".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    a.on_key(key(KeyCode::Enter));
    assert_eq!(a.saved.items.len(), 1);
    assert_eq!(a.saved.items[0].name, "paid orders");
    assert!(a.status.contains("saved as"));
    let items = a.palette_items();
    assert!(
        items
            .iter()
            .any(|i| i.label.ends_with("paid orders") && i.hint == "saved · sql")
    );
    // Choosing it puts its text back in the editor.
    a.tabs[0].editor.set_text("");
    a.on_key(ctrl('k'));
    for c in "paid ord".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    a.on_key(key(KeyCode::Enter));
    assert!(a.tabs[0].editor.text().contains("status = 'paid'"));
}

#[test]
fn the_catalog_library_is_adopted_and_kept_up_to_date() {
    let mut a = app();
    a.saved
        .save("only here", "SELECT 0", Lang::Sql, None)
        .expect("save");
    // The catalog's library arrives; the query only kept here goes there.
    let fx = a.on_msg(Msg::Library(Ok(json!({
        "saved": [{"name": "from studio", "language": "sql", "text": "SELECT 42"}],
        "history": [{"language": "sql", "text": "SELECT 7", "client": "studio"}],
    }))));
    assert!(matches!(&fx[..], [Effect::LibrarySave(q)] if q.name == "only here"));
    assert!(a.saved.shared);
    let items = a.palette_items();
    assert!(items.iter().any(|i| i.label.ends_with("from studio")));
    assert!(
        items
            .iter()
            .any(|i| i.label.ends_with("SELECT 7") && i.hint == "history · sql · studio")
    );
    // Ctrl-S sends the query, with its database.
    a.on_key(ctrl('s'));
    for c in "paid".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    let fx = a.on_key(key(KeyCode::Enter));
    assert!(
        matches!(&fx[..], [Effect::LibrarySave(q)] if q.name == "paid" && q.database.as_deref() == Some("shop"))
    );
    // A run is recorded, redacted, when it ends.
    a.tabs[0].editor = super::editor::Editor::with_text("CREATE USER bob IDENTIFIED BY 'hunter2';");
    a.on_key(key(KeyCode::Enter));
    if a.dialog.is_some() {
        a.on_key(key(KeyCode::Char('y')));
    }
    let fx = a.on_msg(Msg::Done {
        tab: 1,
        error: None,
        elapsed_ms: 5,
        describe: String::new(),
        database: Some("shop".into()),
        in_txn: false,
    });
    let run = fx
        .iter()
        .find_map(|e| match e {
            Effect::LibraryRecord(v) => Some(v.clone()),
            _ => None,
        })
        .expect("recorded");
    let body = run.body();
    assert_eq!(body["client"], "shell");
    assert_eq!(body["ok"], true);
    assert_eq!(body["database"], "shop");
    let text = body["text"].as_str().unwrap_or("");
    assert!(
        text.starts_with("CREATE USER bob") && !text.contains("hunter2"),
        "{text}"
    );
}

#[test]
#[allow(clippy::too_many_lines)] // one walk through the view
fn table_view_filters_stages_edits_and_applies() {
    use super::table::{Purpose, Sub};
    let mut a = app();
    a.focus = Focus::Explorer;
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Down));
    let fx = a.on_key(key(KeyCode::Enter));
    let id = match fx.as_slice() {
        [Effect::TableInfo(id, db)] if db == "shop" => *id,
        other => panic!("{other:?}"),
    };
    // The snapshot gives the key; the first page follows.
    let fx = a.on_msg(Msg::TableInfo(
        id,
        Ok(json!({"columns": {"orders": [
            {"name": "order_id", "type": "int(11)", "key": "PRI", "nullable": false},
            {"name": "status", "type": "varchar(20)", "key": "MUL", "nullable": true}
        ]}})),
    ));
    let sql = "SELECT * FROM `shop`.`orders` ORDER BY `order_id` LIMIT 101".to_owned();
    assert_eq!(
        fx,
        vec![Effect::TableQuery(id, "shop".into(), Purpose::Page, sql)]
    );
    let page = ResultSet {
        columns: vec![
            crate::model::Column::named("order_id"),
            crate::model::Column::named("status"),
        ],
        rows: vec![
            vec![Some("1".into()), Some("open".into())],
            vec![Some("2".into()), Some("paid".into())],
            vec![Some("3".into()), None],
        ],
        ..ResultSet::default()
    };
    a.on_msg(Msg::TableRows(id, Purpose::Page, Ok(vec![page.clone()])));
    // Edit row 1's status, delete row 2, add a row.
    a.on_key(key(KeyCode::Right));
    a.on_key(key(KeyCode::Char('e')));
    for _ in 0..4 {
        a.on_key(key(KeyCode::Backspace));
    }
    for c in "shipped".chars() {
        a.on_key(key(KeyCode::Char(c)));
    }
    a.on_key(key(KeyCode::Enter));
    a.on_key(key(KeyCode::Down));
    a.on_key(key(KeyCode::Char('d')));
    a.on_key(key(KeyCode::Char('o')));
    a.on_key(key(KeyCode::Left));
    a.on_key(key(KeyCode::Enter));
    a.on_key(key(KeyCode::Char('9')));
    a.on_key(key(KeyCode::Enter));
    golden("table_staged.txt", &screen(&mut a, 100, 20));
    // Apply asks first, then runs the three statements in one transaction.
    assert!(a.on_key(key(KeyCode::Char('a'))).is_empty());
    let fx = a.on_key(key(KeyCode::Char('y')));
    let stmts = match fx.as_slice() {
        [Effect::TableApply(i, db, s)] if *i == id && db == "shop" => s.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        stmts.iter().map(|s| s.sql.as_str()).collect::<Vec<_>>(),
        [
            "UPDATE `shop`.`orders` SET `status` = 'shipped' WHERE `order_id` <=> '1' AND `status` <=> 'open' LIMIT 1",
            "DELETE FROM `shop`.`orders` WHERE `order_id` <=> '2' AND `status` <=> 'paid' LIMIT 1",
            "INSERT INTO `shop`.`orders` (`order_id`) VALUES ('9')",
        ]
    );
    // A conflict keeps the changes and shows the row as it is now.
    a.on_msg(Msg::TableApplied(
        id,
        Ok(vec![(
            0,
            Some(vec![Some("1".into()), Some("cancelled".into())]),
        )]),
    ));
    assert!(
        a.status.contains("row 1 is now (1, cancelled)"),
        "{}",
        a.status
    );
    a.on_key(key(KeyCode::Char('t')));
    a.on_key(key(KeyCode::Char('a')));
    let fx = a.on_key(key(KeyCode::Char('y')));
    assert!(
        matches!(&fx[..], [Effect::TableApply(_, _, s)] if s[0].sql.contains("`status` <=> 'cancelled'"))
    );
    // Committed: the page is read again.
    let fx = a.on_msg(Msg::TableApplied(id, Ok(Vec::new())));
    assert!(matches!(
        &fx[..],
        [Effect::TableQuery(_, _, Purpose::Page, _)]
    ));
    assert!(a.status.starts_with("applied 3 changes"));
    // A filter from the cell under the cursor; structure tabs.
    a.on_msg(Msg::TableRows(id, Purpose::Page, Ok(vec![page])));
    a.on_key(key(KeyCode::Right));
    let fx = a.on_key(key(KeyCode::Char('=')));
    assert!(
        matches!(&fx[..], [Effect::TableQuery(_, _, Purpose::Page, q)] if q.contains("WHERE (`status` = 'open')"))
    );
    a.on_key(key(KeyCode::Char(']')));
    golden("table_columns.txt", &screen(&mut a, 100, 16));
    let fx = a.on_key(key(KeyCode::Char(']')));
    assert!(
        matches!(&fx[..], [Effect::TableQuery(_, _, Purpose::Structure(Sub::Indexes), q)] if q == "SHOW INDEX FROM `shop`.`orders`")
    );
}
