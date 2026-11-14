//! `sprig ask`: the statement, the text it prints (golden files; set
//! `SPRIG_UPDATE_GOLDEN=1` to rewrite them after a deliberate change), and
//! a run against a fake gateway that records what it was sent.

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;
use crate::api::{Auth, Caps, DataClient};
use crate::http::HttpClient;
use crate::model::{Column, ResultEvent};
use crate::output::Format;

fn golden(name: &str, got: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/ask/golden")
        .join(name);
    if std::env::var_os("SPRIG_UPDATE_GOLDEN").is_some() {
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

const COLUMNS: [&str; 6] = [
    "query",
    "database",
    "explanation",
    "notes",
    "sources",
    "conversation_id",
];

fn s(x: &str) -> Option<String> {
    Some(x.to_owned())
}

fn answer(rows: Vec<Vec<Option<String>>>) -> ResultSet {
    ResultSet::from_events(&[
        ResultEvent::Header(COLUMNS.iter().map(|c| Column::named(*c)).collect()),
        ResultEvent::Rows(rows),
        ResultEvent::End(crate::model::EndInfo::default()),
    ])
    .remove(0)
}

fn two_queries() -> ResultSet {
    answer(vec![
        vec![
            s("SELECT id, total FROM orders WHERE total > 100"),
            s("shop"),
            s("The orders over 100."),
            s("Reads only. Statement 1 of 2 in one proposal."),
            s("table: shop.orders"),
            s("42"),
        ],
        vec![
            s("DELETE FROM orders WHERE total = 0"),
            s("shop"),
            s("Then removes the empty ones."),
            s(
                "Changes data, schema or settings, or isn't a plain query: review it before you run it. Statement 2 of 2 in one proposal.",
            ),
            s("table: shop.orders"),
            s("42"),
        ],
    ])
}

#[test]
fn the_statement_quotes_the_question() {
    assert_eq!(statement("top customers", None), "ASK 'top customers'");
    assert_eq!(
        statement("it's a \\ test", Some("dba")),
        "ASK 'it''s a \\\\ test' ROLE dba"
    );
}

#[test]
fn queries_print_with_their_explanation() {
    let rows = rows(&two_queries()).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].conversation.as_deref(), Some("42"));
    golden("two_queries.txt", &render(&rows));
    assert_eq!(
        footer(&rows, false),
        "Nothing ran: --run runs it (anything but a read asks first). Conversation 42."
    );
    assert_eq!(footer(&rows, true), "Conversation 42.");
}

#[test]
fn an_answer_in_words_prints_as_words() {
    let set = answer(vec![vec![
        None,
        None,
        s("That needs a table of exchange rates, which this tenant doesn't have."),
        s("No query: the assistant answered in words."),
        None,
        s("43"),
    ]]);
    let rows = rows(&set).unwrap();
    golden("words.txt", &render(&rows));
    assert_eq!(footer(&rows, false), "Conversation 43.");
    // One query: no "1 of 1".
    let one = answer(vec![vec![
        s("SELECT 1;"),
        None,
        s("One."),
        None,
        None,
        s("44"),
    ]]);
    assert_eq!(
        render(&super::rows(&one).unwrap()),
        "-- Query\nSELECT 1;\n\nOne.\n"
    );
}

#[test]
fn an_older_gateways_answer_is_refused() {
    let set = ResultSet::from_events(&[
        ResultEvent::Header(vec![Column::named("x")]),
        ResultEvent::End(crate::model::EndInfo::default()),
    ])
    .remove(0);
    assert!(rows(&set).unwrap_err().is(ShellCode::Parse));
}

#[test]
fn only_reads_run_without_asking() {
    assert_eq!(needs_confirm("SELECT 1"), None);
    assert_eq!(needs_confirm("WITH a AS (SELECT 1) SELECT * FROM a"), None);
    assert_eq!(
        needs_confirm("DELETE FROM t WHERE id = 1").as_deref(),
        Some("it changes data")
    );
    assert_eq!(
        needs_confirm("DELETE FROM t").as_deref(),
        Some("DELETE without WHERE is destructive")
    );
    assert_eq!(
        needs_confirm("CREATE INDEX i ON t (a)").as_deref(),
        Some("it changes the schema")
    );
    assert!(needs_confirm("SPRIG SET x = 1").is_some());
}

/// A gateway that answers `ASK` with two queries and records every
/// statement it is sent.
async fn fake_gateway(seen: Arc<Mutex<Vec<(String, String)>>>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut c, _)) = l.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    // One request: headers, then the body.
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        let mut chunk = [0u8; 4096];
                        match c.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let path = head
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + len {
                        let mut chunk = [0u8; 4096];
                        match c.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body: Value =
                        serde_json::from_slice(&buf[head_end..head_end + len]).unwrap_or_default();
                    buf.drain(..head_end + len);
                    let sql = body["sql"].as_str().unwrap_or_default().to_owned();
                    seen.lock().unwrap().push((path, sql.clone()));
                    let lines: Vec<Value> = if sql.starts_with("ASK ") {
                        let rows: Vec<Vec<Option<String>>> = two_queries().rows;
                        vec![
                            json!({"header": {"columns": COLUMNS.iter().map(|c| json!({"name": c, "type": "VARCHAR"})).collect::<Vec<_>>()}}),
                            json!({"rows": {"rows": rows}}),
                            json!({"end": {}}),
                        ]
                    } else if sql.starts_with("SELECT") {
                        vec![
                            json!({"header": {"columns": [{"name": "id", "type": "BIGINT"}]}}),
                            json!({"rows": {"rows": [["7"]]}}),
                            json!({"end": {}}),
                        ]
                    } else {
                        vec![json!({"end": {"affected_rows": "1"}})]
                    };
                    let mut out = String::new();
                    for l in lines {
                        out.push_str(&l.to_string());
                        out.push('\n');
                    }
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n{out}",
                        out.len()
                    );
                    if c.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn run_runs_the_read_and_holds_the_write_without_a_confirm() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let url = fake_gateway(Arc::clone(&seen)).await;
    let client = DataClient::new(HttpClient::new(&url, None).unwrap(), Auth::None);
    let mut runner = Runner::new(client, None, Caps::default());
    let m = crate::cli::command()
        .try_get_matches_from([
            "sprig", "ask", "orders", "over", "100", "--run", "-o", "tsv",
        ])
        .unwrap();
    let Some(("ask", sub)) = m.subcommand() else {
        panic!("no ask subcommand");
    };
    let mut w = Writer::new(Vec::new(), Format::Tsv);
    let class = cli(&mut runner, &m, sub, &mut w, true, &mut |_| false).await;
    // The write wasn't confirmed (tests have no terminal): 39982's class.
    assert_eq!(class, ExitClass::Confirm);
    let sent = seen.lock().unwrap().clone();
    assert_eq!(
        sent,
        [
            (
                "/v1/databases/-/sql:query".to_owned(),
                "ASK 'orders over 100'".to_owned()
            ),
            (
                "/v1/databases/shop/sql:query".to_owned(),
                "SELECT id, total FROM orders WHERE total > 100".to_owned()
            ),
        ]
    );
    // The read ran in the proposal's database.
    assert_eq!(runner.database.as_deref(), Some("shop"));
    let out = String::from_utf8(w.into_inner()).unwrap();
    assert!(out.contains("SELECT id, total FROM orders WHERE total > 100\tshop"));
    assert!(out.ends_with("id\n7\n"), "{out}");

    // With --yes the write runs too; without --run nothing does.
    seen.lock().unwrap().clear();
    let m = crate::cli::command()
        .try_get_matches_from(["sprig", "--yes", "ask", "orders", "--run", "--role", "dba"])
        .unwrap();
    let Some(("ask", sub)) = m.subcommand() else {
        panic!("no ask subcommand");
    };
    let mut w = Writer::new(Vec::new(), Format::Tsv);
    assert_eq!(
        cli(&mut runner, &m, sub, &mut w, false, &mut |_| false).await,
        ExitClass::Ok
    );
    let sent: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(_, s)| s.clone())
        .collect();
    assert_eq!(
        sent,
        [
            "ASK 'orders' ROLE dba",
            "SELECT id, total FROM orders WHERE total > 100",
            "DELETE FROM orders WHERE total = 0",
        ]
    );

    seen.lock().unwrap().clear();
    let m = crate::cli::command()
        .try_get_matches_from(["sprig", "ask", "orders"])
        .unwrap();
    let Some(("ask", sub)) = m.subcommand() else {
        panic!("no ask subcommand");
    };
    let mut w = Writer::new(Vec::new(), Format::Tsv);
    assert_eq!(
        cli(&mut runner, &m, sub, &mut w, false, &mut |_| false).await,
        ExitClass::Ok
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}
