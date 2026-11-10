//! The multi-line editor (chapter 53, "Completion and highlighting").
//!
//! Text is kept as lines of `char`s, so the cursor moves by characters.
//! Highlighting comes from the shell's own lexer.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use crate::lang::{Lang, Tok, is_keyword, lex};

/// The editor's text and cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Editor {
    lines: Vec<Vec<char>>,
    /// Cursor row.
    pub row: usize,
    /// Cursor column, in characters.
    pub col: usize,
    /// First visible row.
    pub scroll: usize,
}

impl Default for Editor {
    fn default() -> Self {
        Self {
            lines: vec![Vec::new()],
            row: 0,
            col: 0,
            scroll: 0,
        }
    }
}

impl Editor {
    /// An editor holding `text`, the cursor at its end.
    #[must_use]
    pub fn with_text(text: &str) -> Self {
        let mut e = Self::default();
        e.set_text(text);
        e
    }

    /// The whole text.
    #[must_use]
    pub fn text(&self) -> String {
        self.lines
            .iter()
            .map(|l| l.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Replaces the text; the cursor goes to the end.
    pub fn set_text(&mut self, text: &str) {
        self.lines = text.split('\n').map(|l| l.chars().collect()).collect();
        if self.lines.is_empty() {
            self.lines.push(Vec::new());
        }
        self.row = self.lines.len() - 1;
        self.col = self.lines[self.row].len();
    }

    /// Whether there is no text.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.len() == 1 && self.lines[0].is_empty()
    }

    /// The number of lines.
    #[must_use]
    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    fn clamp(&mut self) {
        self.row = self.row.min(self.lines.len() - 1);
        self.col = self.col.min(self.lines[self.row].len());
    }

    /// Inserts a character.
    pub fn insert(&mut self, c: char) {
        if c == '\n' {
            self.newline();
            return;
        }
        self.lines[self.row].insert(self.col, c);
        self.col += 1;
    }

    /// Inserts text (a paste or a completion).
    pub fn insert_str(&mut self, s: &str) {
        for c in s.chars() {
            if c != '\r' {
                self.insert(c);
            }
        }
    }

    /// Splits the line at the cursor, keeping the indentation.
    pub fn newline(&mut self) {
        let rest = self.lines[self.row].split_off(self.col);
        let indent: Vec<char> = self.lines[self.row]
            .iter()
            .take_while(|c| **c == ' ')
            .copied()
            .collect();
        self.row += 1;
        self.col = indent.len();
        let mut line = indent;
        line.extend(rest);
        self.lines.insert(self.row, line);
    }

    /// Deletes the character before the cursor.
    pub fn backspace(&mut self) {
        if self.col > 0 {
            self.col -= 1;
            self.lines[self.row].remove(self.col);
        } else if self.row > 0 {
            let line = self.lines.remove(self.row);
            self.row -= 1;
            self.col = self.lines[self.row].len();
            self.lines[self.row].extend(line);
        }
    }

    /// Deletes the character under the cursor.
    pub fn delete(&mut self) {
        if self.col < self.lines[self.row].len() {
            self.lines[self.row].remove(self.col);
        } else if self.row + 1 < self.lines.len() {
            let next = self.lines.remove(self.row + 1);
            self.lines[self.row].extend(next);
        }
    }

    /// Moves the cursor.
    pub fn left(&mut self) {
        if self.col > 0 {
            self.col -= 1;
        } else if self.row > 0 {
            self.row -= 1;
            self.col = self.lines[self.row].len();
        }
    }

    /// Moves the cursor.
    pub fn right(&mut self) {
        if self.col < self.lines[self.row].len() {
            self.col += 1;
        } else if self.row + 1 < self.lines.len() {
            self.row += 1;
            self.col = 0;
        }
    }

    /// Moves up a line; `false` at the first line.
    pub fn up(&mut self) -> bool {
        if self.row == 0 {
            return false;
        }
        self.row -= 1;
        self.clamp();
        true
    }

    /// Moves down a line; `false` at the last line.
    pub fn down(&mut self) -> bool {
        if self.row + 1 >= self.lines.len() {
            return false;
        }
        self.row += 1;
        self.clamp();
        true
    }

    /// Start of the line.
    pub fn home(&mut self) {
        self.col = 0;
    }

    /// End of the line.
    pub fn end(&mut self) {
        self.col = self.lines[self.row].len();
    }

    /// One word left.
    pub fn word_left(&mut self) {
        let l = &self.lines[self.row];
        let mut c = self.col;
        while c > 0 && !l[c - 1].is_alphanumeric() {
            c -= 1;
        }
        while c > 0 && (l[c - 1].is_alphanumeric() || l[c - 1] == '_') {
            c -= 1;
        }
        self.col = c;
    }

    /// One word right.
    pub fn word_right(&mut self) {
        let l = &self.lines[self.row];
        let mut c = self.col;
        while c < l.len() && (l[c].is_alphanumeric() || l[c] == '_') {
            c += 1;
        }
        while c < l.len() && !l[c].is_alphanumeric() {
            c += 1;
        }
        self.col = c;
    }

    /// Deletes to the start of the line (`Ctrl-U`).
    pub fn kill_to_start(&mut self) {
        self.lines[self.row].drain(..self.col);
        self.col = 0;
    }

    /// Deletes to the end of the line (`Ctrl-K`).
    pub fn kill_to_end(&mut self) {
        self.lines[self.row].truncate(self.col);
    }

    /// The identifier being typed before the cursor (letters, digits, `_`,
    /// `$` and `.`), for completion.
    #[must_use]
    pub fn word_before_cursor(&self) -> String {
        let l = &self.lines[self.row];
        let mut c = self.col;
        while c > 0 && (l[c - 1].is_alphanumeric() || matches!(l[c - 1], '_' | '$' | '.')) {
            c -= 1;
        }
        l[c..self.col].iter().collect()
    }

    /// Replaces the last `n` characters before the cursor with `s`.
    pub fn replace_before_cursor(&mut self, n: usize, s: &str) {
        let n = n.min(self.col);
        self.lines[self.row].drain(self.col - n..self.col);
        self.col -= n;
        self.insert_str(s);
    }

    /// The cursor as a byte offset into [`Editor::text`].
    #[must_use]
    pub fn cursor_offset(&self) -> usize {
        let mut off = 0;
        for l in &self.lines[..self.row] {
            off += l.iter().map(|c| c.len_utf8()).sum::<usize>() + 1;
        }
        off + self.lines[self.row][..self.col]
            .iter()
            .map(|c| c.len_utf8())
            .sum::<usize>()
    }

    /// Whether the cursor is at the very end and the text ends a
    /// statement (Enter then runs it, as in `mysql`).
    #[must_use]
    pub fn complete_at_end(&self, lang: Lang) -> bool {
        let at_end = self.row + 1 == self.lines.len() && self.col == self.lines[self.row].len();
        if !at_end {
            return false;
        }
        let t = self.text();
        match lang {
            Lang::Sql | Lang::Cypher => {
                t.trim_end().ends_with(';') && !crate::lang::open_at_end(&t, lang == Lang::Cypher)
            }
            Lang::Redis => !t.trim().is_empty() && crate::lang::redis_argv(&t).is_ok(),
            Lang::Mongo => {
                !t.trim().is_empty() && crate::lang::split_mongo(&t).is_ok_and(|s| !s.is_empty())
            }
        }
    }

    /// The statement under the cursor (SQL and Cypher: between `;`s;
    /// Redis: the line; MongoDB: the call chain), or the whole text.
    #[must_use]
    pub fn statement_at_cursor(&self, lang: Lang) -> String {
        let text = self.text();
        let off = self.cursor_offset();
        let stmts = match lang {
            Lang::Redis => {
                return self.lines[self.row]
                    .iter()
                    .collect::<String>()
                    .trim()
                    .to_owned();
            }
            _ => crate::lang::split(&text, lang),
        };
        let Ok(stmts) = stmts else {
            return text.trim().to_owned();
        };
        if stmts.len() <= 1 {
            return stmts.first().map_or_else(String::new, |s| s.text.clone());
        }
        // The statement whose line range holds the cursor's line; the last
        // one that starts at or before it.
        let line = text[..off].bytes().filter(|&b| b == b'\n').count() + 1;
        let mut pick = &stmts[0];
        for s in &stmts {
            if s.line <= line {
                pick = s;
            }
        }
        pick.text.clone()
    }

    /// Keeps the cursor row visible in `height` rows.
    pub fn scroll_to_cursor(&mut self, height: usize) {
        if height == 0 {
            return;
        }
        if self.row < self.scroll {
            self.scroll = self.row;
        } else if self.row >= self.scroll + height {
            self.scroll = self.row + 1 - height;
        }
    }

    /// Highlighted lines `scroll..scroll+height`, with line numbers.
    #[must_use]
    pub fn render_lines(&self, lang: Lang, height: usize) -> Vec<Line<'static>> {
        let text = self.text();
        let spans = highlight(&text, lang);
        // Split the highlighted spans back into lines.
        let mut lines: Vec<Vec<Span<'static>>> = vec![Vec::new()];
        for (style, s) in spans {
            let mut first = true;
            for part in s.split('\n') {
                if !first {
                    lines.push(Vec::new());
                }
                first = false;
                if !part.is_empty()
                    && let Some(last) = lines.last_mut()
                {
                    last.push(Span::styled(part.to_owned(), style));
                }
            }
        }
        let width = self.lines.len().to_string().len().max(2);
        lines
            .into_iter()
            .enumerate()
            .skip(self.scroll)
            .take(height)
            .map(|(i, mut l)| {
                l.insert(
                    0,
                    Span::styled(
                        format!("{:>width$} ", i + 1),
                        Style::new().fg(Color::DarkGray),
                    ),
                );
                Line::from(l)
            })
            .collect()
    }

    /// The gutter width (line numbers and a space).
    #[must_use]
    pub fn gutter(&self) -> usize {
        self.lines.len().to_string().len().max(2) + 1
    }

    /// The cursor's display column (characters before it, counted wide
    /// characters as their width).
    #[must_use]
    pub fn cursor_display_col(&self) -> usize {
        use unicode_width::UnicodeWidthChar;
        self.lines[self.row][..self.col]
            .iter()
            .map(|c| c.width().unwrap_or(0))
            .sum()
    }
}

/// Styles per token kind.
#[must_use]
pub fn style_of(kind: Tok, keyword: bool) -> Style {
    match kind {
        Tok::Word if keyword => Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        Tok::Str => Style::new().fg(Color::Green),
        Tok::Number => Style::new().fg(Color::Magenta),
        Tok::Comment => Style::new()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
        Tok::Quoted => Style::new().fg(Color::Yellow),
        _ => Style::new(),
    }
}

/// The text cut into styled pieces.
#[must_use]
pub fn highlight(text: &str, lang: Lang) -> Vec<(Style, String)> {
    match lang {
        Lang::Sql | Lang::Cypher => lex(text, lang == Lang::Cypher)
            .into_iter()
            .map(|t| {
                let s = t.text(text);
                (
                    style_of(t.kind, t.kind == Tok::Word && is_keyword(s, lang)),
                    s.to_owned(),
                )
            })
            .collect(),
        Lang::Redis => {
            // The first word of each line is the command.
            let mut out = Vec::new();
            for (i, line) in text.split('\n').enumerate() {
                if i > 0 {
                    out.push((Style::new(), "\n".to_owned()));
                }
                let trimmed = line.trim_start();
                let lead = &line[..line.len() - trimmed.len()];
                let cmd_end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
                out.push((Style::new(), lead.to_owned()));
                out.push((style_of(Tok::Word, true), trimmed[..cmd_end].to_owned()));
                for t in lex(&trimmed[cmd_end..], false) {
                    let s = t.text(&trimmed[cmd_end..]);
                    out.push((style_of(t.kind, false), s.to_owned()));
                }
            }
            out
        }
        Lang::Mongo => lex(text, true)
            .into_iter()
            .map(|t| {
                let s = t.text(text);
                (
                    style_of(t.kind, t.kind == Tok::Word && is_keyword(s, lang)),
                    s.to_owned(),
                )
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editing() {
        let mut e = Editor::default();
        e.insert_str("SELECT 1;\n  SELECT 2;");
        assert_eq!(e.text(), "SELECT 1;\n  SELECT 2;");
        assert_eq!((e.row, e.col), (1, 11));
        e.newline();
        assert_eq!(e.col, 2, "keeps the indentation");
        e.backspace();
        e.backspace();
        e.backspace();
        assert_eq!(e.text(), "SELECT 1;\n  SELECT 2;");
        assert!(e.complete_at_end(Lang::Sql));
        e.up();
        assert_eq!(e.statement_at_cursor(Lang::Sql), "SELECT 1");
        e.down();
        assert_eq!(e.statement_at_cursor(Lang::Sql), "SELECT 2");
        e.home();
        e.word_right();
        assert_eq!(e.col, 2);
        e.kill_to_end();
        assert_eq!(e.text(), "SELECT 1;\n  ");
    }

    #[test]
    fn words_and_offsets() {
        let mut e = Editor::with_text("SELECT * FROM shop.ord");
        assert_eq!(e.word_before_cursor(), "shop.ord");
        e.replace_before_cursor(3, "orders");
        assert_eq!(e.text(), "SELECT * FROM shop.orders");
        let e = Editor::with_text("é\nab");
        assert_eq!(e.cursor_offset(), "é\nab".len());
        assert!(!Editor::with_text("SELECT 'a;").complete_at_end(Lang::Sql));
    }
}
