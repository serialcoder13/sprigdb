//! Terminal helpers for the CLI: hidden password input and yes/no
//! questions.

use std::io::{IsTerminal, Write};

use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, read};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

/// Whether standard input and standard error are terminals, so we can ask.
#[must_use]
pub fn interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

/// Reads a line without echo; `None` when not on a terminal or on `Esc`
/// or `Ctrl-C`.
#[must_use]
pub fn prompt_hidden(prompt: &str) -> Option<String> {
    if !interactive() {
        return None;
    }
    eprint!("{prompt}");
    let _ = std::io::stderr().flush();
    if enable_raw_mode().is_err() {
        return None;
    }
    let mut s = String::new();
    let result = loop {
        match read() {
            Ok(Event::Key(k)) if k.kind != KeyEventKind::Release => match k.code {
                KeyCode::Enter => break Some(std::mem::take(&mut s)),
                KeyCode::Esc => break None,
                KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break None,
                KeyCode::Char('u') if k.modifiers.contains(KeyModifiers::CONTROL) => s.clear(),
                KeyCode::Backspace => {
                    s.pop();
                }
                KeyCode::Char(c) => s.push(c),
                _ => {}
            },
            Ok(_) => {}
            Err(_) => break None,
        }
    };
    let _ = disable_raw_mode();
    eprintln!();
    result
}

/// Reads one line from standard input (for `--password-stdin`).
#[must_use]
pub fn read_stdin_line() -> Option<String> {
    let mut s = String::new();
    std::io::stdin().read_line(&mut s).ok()?;
    let s = s.trim_end_matches(['\n', '\r']).to_owned();
    (!s.is_empty()).then_some(s)
}

/// Asks a yes/no question on the terminal; `false` when not interactive.
#[must_use]
pub fn confirm(question: &str) -> bool {
    if !interactive() {
        return false;
    }
    eprint!("{question} [y/N] ");
    let _ = std::io::stderr().flush();
    let mut s = String::new();
    if std::io::stdin().read_line(&mut s).is_err() {
        return false;
    }
    matches!(s.trim(), "y" | "Y" | "yes" | "YES")
}
