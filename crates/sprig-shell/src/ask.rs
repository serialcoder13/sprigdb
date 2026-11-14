//! `sprig ask "<question>" [--role dba] [--run]`: the gateway's `ASK`
//! statement from the command line.
//!
//! The question goes to the gateway as `ASK '<question>' [ROLE dba]` over
//! the chosen transport, and the answer comes back as rows (`query`,
//! `database`, `explanation`, `notes`, `sources`, `conversation_id`). The
//! shell prints them as text, or as rows with `-o`. Nothing runs unless
//! `--run` is given; then each query runs in its database, and anything but
//! a read asks first on a terminal, or needs `--yes` in a script (39982).

use std::fmt::Write as _;
use std::io::Write;

use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::cli::run_cancellable;
use crate::error::{ExitClass, Result, ShellCode, ShellError};
use crate::exec::Runner;
use crate::lang::{Class, Lang, classify_sql, destructive};
use crate::model::{Cell, Collect, ResultSet, Sink};
use crate::output::Writer;

/// The `ask` subcommand.
#[must_use]
pub fn command() -> Command {
    Command::new("ask")
        .about("Ask an AI assistant for a query (ASK); nothing runs without --run")
        .arg(
            Arg::new("question")
                .required(true)
                .num_args(1..)
                .help("The question, in plain words"),
        )
        .arg(
            Arg::new("role")
                .long("role")
                .value_parser(["data", "dba"])
                .help("Which assistant answers [default: data]"),
        )
        .arg(
            Arg::new("run")
                .long("run")
                .action(ArgAction::SetTrue)
                .help("Run the proposed queries (anything but a read asks first)"),
        )
}

/// The `ASK` statement for `question`.
#[must_use]
pub fn statement(question: &str, role: Option<&str>) -> String {
    let mut s = String::from("ASK '");
    for c in question.chars() {
        match c {
            '\'' => s.push_str("''"),
            '\\' => s.push_str("\\\\"),
            c => s.push(c),
        }
    }
    s.push('\'');
    if let Some(r) = role {
        let _ = write!(s, " ROLE {r}");
    }
    s
}

/// One row of the answer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AskRow {
    /// One statement, or `None` when the answer has none.
    pub query: Option<String>,
    /// The database it is meant for.
    pub database: Option<String>,
    /// Why.
    pub explanation: String,
    /// Reads only, the estimate, a cut-off answer.
    pub notes: Option<String>,
    /// What the answer drew on.
    pub sources: Option<String>,
    /// The stored conversation.
    pub conversation: Option<String>,
}

/// The rows of an `ASK` result.
///
/// # Errors
/// 39985 when the result isn't an `ASK` answer (an older gateway).
pub fn rows(set: &ResultSet) -> Result<Vec<AskRow>> {
    let at = |n: &str| set.columns.iter().position(|c| c.name == n);
    let (Some(q), Some(e)) = (at("query"), at("explanation")) else {
        return Err(ShellError::shell(
            ShellCode::Parse,
            "the gateway's answer to ASK has no query and explanation columns (is it older than this shell?)",
        ));
    };
    let cell = |row: &[Cell], i: Option<usize>| -> Option<String> {
        i.and_then(|i| row.get(i).cloned().flatten())
            .filter(|s| !s.trim().is_empty())
    };
    Ok(set
        .rows
        .iter()
        .map(|r| AskRow {
            query: cell(r, Some(q)),
            database: cell(r, at("database")),
            explanation: cell(r, Some(e)).unwrap_or_default(),
            notes: cell(r, at("notes")),
            sources: cell(r, at("sources")),
            conversation: cell(r, at("conversation_id")),
        })
        .collect())
}

/// The answer as text: each query with its explanation, notes and sources.
#[must_use]
pub fn render(rows: &[AskRow]) -> String {
    let mut out = String::new();
    let n = rows.iter().filter(|r| r.query.is_some()).count();
    let mut i = 0;
    for r in rows {
        if !out.is_empty() {
            out.push('\n');
        }
        if let Some(q) = &r.query {
            i += 1;
            let mut head = if n == 1 {
                "-- Query".to_owned()
            } else {
                format!("-- Query {i} of {n}")
            };
            if let Some(d) = &r.database {
                let _ = write!(head, " (database {d})");
            }
            let _ = writeln!(out, "{head}\n{};\n", q.trim_end_matches(';'));
        }
        if !r.explanation.is_empty() {
            let _ = writeln!(out, "{}", r.explanation.trim_end());
        }
        if let Some(notes) = &r.notes {
            let _ = writeln!(out, "Notes: {notes}");
        }
        if let Some(s) = &r.sources {
            let _ = writeln!(out, "Sources: {s}");
        }
    }
    out
}

/// The line after the answer, on standard error.
#[must_use]
pub fn footer(rows: &[AskRow], ran: bool) -> String {
    let conv = rows
        .iter()
        .find_map(|r| r.conversation.as_deref())
        .map(|c| format!(" Conversation {c}."))
        .unwrap_or_default();
    if ran || rows.iter().all(|r| r.query.is_none()) {
        return conv.trim_start().to_owned();
    }
    format!("Nothing ran: --run runs it (anything but a read asks first).{conv}")
}

/// Why `query` must be confirmed before it runs, or `None` for a read.
#[must_use]
pub fn needs_confirm(query: &str) -> Option<String> {
    if let Some(d) = destructive(query, Lang::Sql) {
        return Some(format!("{d} is destructive"));
    }
    match classify_sql(query) {
        Class::Read => None,
        Class::Write => Some("it changes data".to_owned()),
        Class::Ddl => Some("it changes the schema".to_owned()),
        Class::Account => Some("it changes accounts or grants".to_owned()),
        Class::Transaction | Class::Other => Some("it isn't a plain read".to_owned()),
    }
}

/// Runs `sprig ask`: asks, prints, and with `--run` runs. `confirm` asks
/// the user a yes/no question (on a terminal; `false` elsewhere).
#[allow(clippy::too_many_lines)] // ask, print, then run each query
pub async fn cli(
    runner: &mut Runner,
    m: &ArgMatches,
    sub: &ArgMatches,
    w: &mut Writer<impl Write + Send>,
    rows_format: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> ExitClass {
    let question = sub
        .get_many::<String>("question")
        .map(|v| v.cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    let stmt = statement(&question, sub.get_one::<String>("role").map(String::as_str));
    let mut got = Collect::default();
    if let Err(e) = runner.run(&stmt, Lang::Sql, &mut got).await {
        eprintln!("sprig: {e}");
        return e.class();
    }
    let sets = ResultSet::from_events(&got.0);
    let answer = match sets.iter().find(|s| !s.columns.is_empty()) {
        Some(s) => s,
        None => {
            let e = ShellError::shell(ShellCode::Parse, "the gateway sent no answer to ASK");
            eprintln!("sprig: {e}");
            return e.class();
        }
    };
    let rows = match rows(answer) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sprig: {e}");
            return e.class();
        }
    };
    if rows_format {
        for ev in got.0 {
            if w.event(ev).is_err() {
                return ExitClass::Ok;
            }
        }
        let _ = w.finish();
    } else {
        print!("{}", render(&rows));
        let _ = std::io::stdout().flush();
    }
    let run = sub.get_flag("run");
    if !run {
        let f = footer(&rows, false);
        if !f.is_empty() {
            eprintln!("{f}");
        }
        return ExitClass::Ok;
    }
    let queries: Vec<&AskRow> = rows.iter().filter(|r| r.query.is_some()).collect();
    if queries.is_empty() {
        eprintln!("There is no query to run.");
        return ExitClass::Ok;
    }
    let yes = m.get_flag("yes") || sub.get_flag("yes");
    let mut first = ExitClass::Ok;
    for r in queries {
        let q = r.query.as_deref().unwrap_or_default();
        if let Some(why) = needs_confirm(q) {
            let ok = yes || confirm(&format!("\n{q};\n\nThe assistant's query: {why}. Run it?"));
            if !ok {
                let e = ShellError::shell(
                    ShellCode::ConfirmationRequired,
                    format!("not run: {why}; confirm on a terminal or pass --yes"),
                );
                eprintln!("sprig: {e}");
                if first == ExitClass::Ok {
                    first = e.class();
                }
                continue;
            }
        }
        if let Some(db) = &r.database
            && runner.database.as_deref() != Some(db.as_str())
        {
            // REST names the database on each call; a native connection
            // switches with USE.
            if runner.native.is_none() {
                runner.database = Some(db.clone());
            } else if let Err(e) = runner
                .run(
                    &format!("USE `{}`", db.replace('`', "``")),
                    Lang::Sql,
                    &mut Collect::default(),
                )
                .await
            {
                eprintln!("sprig: {e}");
                if first == ExitClass::Ok {
                    first = e.class();
                }
                continue;
            }
        }
        if !rows_format {
            println!();
        }
        let r = run_cancellable(runner, q, Lang::Sql, &mut *w).await;
        let _ = w.finish();
        if let Err(e) = r {
            eprintln!("sprig: {e}");
            if first == ExitClass::Ok {
                first = e.class();
            }
            if e.is(ShellCode::Cancelled) {
                break;
            }
        }
    }
    let f = footer(&rows, true);
    if !f.is_empty() {
        eprintln!("{f}");
    }
    first
}

#[cfg(test)]
mod tests;
