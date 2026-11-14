//! Text helpers for the row-level security rewrites: sqlparser-rs
//! positions (line and character column, from 1) to byte offsets, the
//! statement's significant tokens, and span edits.
//!
//! Both rewrites ([`super::expr`] and [`super::inject`]) parse the name layer's
//! parse view of a text (same characters on the same lines, comments blanked,
//! strings as `x`), so an AST position names the same place in the original
//! text, and every change is an insertion or replacement at token boundaries of
//! the original.

use sprig_common::SprigError;
use sqlparser::tokenizer::Location;

use crate::lex::{Token, TokenKind};

/// Byte offsets of `(line, column)` positions in one text.
pub(crate) struct Positions<'a> {
    text: &'a str,
    line_starts: Vec<usize>,
}

impl<'a> Positions<'a> {
    pub(crate) fn new(text: &'a str) -> Self {
        let mut line_starts = vec![0];
        for (i, b) in text.bytes().enumerate() {
            if b == b'\n' {
                line_starts.push(i + 1);
            }
        }
        Self { text, line_starts }
    }

    /// The byte offset of a location; `None` for an empty or out-of-range
    /// one.
    pub(crate) fn offset(&self, at: Location) -> Option<usize> {
        if at.line == 0 || at.column == 0 {
            return None;
        }
        let ls = *self.line_starts.get(usize::try_from(at.line - 1).ok()?)?;
        let line_end = self.text[ls..]
            .find('\n')
            .map_or(self.text.len(), |e| ls + e);
        let want = usize::try_from(at.column - 1).ok()?;
        let mut n = 0usize;
        for (i, _) in self.text[ls..line_end].char_indices() {
            if n == want {
                return Some(ls + i);
            }
            n += 1;
        }
        (n == want).then_some(line_end)
    }

    /// The location of a byte offset (inverse of [`Self::offset`]).
    pub(crate) fn location(&self, off: usize) -> Location {
        let line = match self.line_starts.binary_search(&off) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let ls = self.line_starts[line];
        let col = self.text[ls..off.min(self.text.len())].chars().count();
        Location {
            line: line as u64 + 1,
            column: col as u64 + 1,
        }
    }
}

/// One edit of a text: replace `start..end` with `with`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Edit {
    pub start: usize,
    pub end: usize,
    pub with: String,
}

/// Applies edits (any order; equal starts keep their order) to `src`.
///
/// # Errors
/// 39366 if two edits overlap (a rewrite bug: fail closed).
pub(crate) fn apply(src: &str, edits: &[Edit]) -> Result<String, SprigError> {
    let mut e: Vec<&Edit> = edits.iter().collect();
    e.sort_by_key(|x| (x.start, x.end));
    let mut out = String::with_capacity(src.len() + 64);
    let mut at = 0usize;
    for x in e {
        if x.start < at || x.end < x.start || x.end > src.len() {
            return Err(super::unsupported("overlapping edits"));
        }
        out.push_str(&src[at..x.start]);
        out.push_str(&x.with);
        at = x.end;
    }
    out.push_str(&src[at..]);
    Ok(out)
}

/// Maps edits made on one text to another with the same lines and
/// characters (a parse view and its original).
pub(crate) fn map_edits(
    from: &Positions<'_>,
    to: &Positions<'_>,
    edits: &[Edit],
) -> Option<Vec<Edit>> {
    edits
        .iter()
        .map(|e| {
            Some(Edit {
                start: to
                    .offset(from.location(e.start))
                    .or_else(|| end_of(to, from, e.start))?,
                end: to
                    .offset(from.location(e.end))
                    .or_else(|| end_of(to, from, e.end))?,
                with: e.with.clone(),
            })
        })
        .collect()
}

/// An offset at the very end of a line or text maps to the same place.
fn end_of(to: &Positions<'_>, from: &Positions<'_>, off: usize) -> Option<usize> {
    let l = from.location(off);
    let prev = Location {
        line: l.line,
        column: l.column.checked_sub(1)?,
    };
    let p = to.offset(prev)?;
    let ch = to.text[p..].chars().next()?;
    Some(p + ch.len_utf8())
}

/// The significant tokens of a text, with helpers.
pub(crate) struct Toks<'a> {
    pub src: &'a [u8],
    pub toks: Vec<Token>,
}

impl<'a> Toks<'a> {
    pub(crate) fn new(src: &'a [u8], all: &[Token]) -> Self {
        Self {
            src,
            toks: all
                .iter()
                .copied()
                .filter(|t| !t.kind.is_trivia())
                .collect(),
        }
    }

    /// The index of the token starting at byte `off`.
    pub(crate) fn at(&self, off: usize) -> Option<usize> {
        self.toks
            .binary_search_by_key(&off, |t| t.start as usize)
            .ok()
    }

    pub(crate) fn word(&self, i: usize, w: &str) -> bool {
        self.toks.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.src).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    pub(crate) fn punct(&self, i: usize, p: &str) -> bool {
        self.toks.get(i).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(self.src) == p.as_bytes()
        })
    }

    /// The index of the `)` matching the `(` at `open`.
    pub(crate) fn matching(&self, open: usize) -> Option<usize> {
        let mut depth = 0usize;
        for i in open..self.toks.len() {
            if self.punct(i, "(") {
                depth += 1;
            } else if self.punct(i, ")") {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        None
    }

    /// From token `i`, the first token at the same parenthesis depth for
    /// which `stop` holds, or where the depth drops below the start; the
    /// token count if the text ends first.
    pub(crate) fn scan(&self, i: usize, stop: impl Fn(&Self, usize) -> bool) -> usize {
        let mut depth = 0i64;
        let mut k = i;
        while k < self.toks.len() {
            if self.punct(k, "(") {
                depth += 1;
            } else if self.punct(k, ")") {
                depth -= 1;
                if depth < 0 {
                    return k;
                }
            } else if depth == 0 && stop(self, k) {
                return k;
            }
            k += 1;
        }
        k
    }

    /// Byte offset of token `i`'s start (the text's end past the last).
    pub(crate) fn start(&self, i: usize) -> usize {
        self.toks
            .get(i)
            .map_or(self.src.len(), |t| t.start as usize)
    }

    /// Byte offset just past token `i`.
    pub(crate) fn end(&self, i: usize) -> usize {
        self.toks.get(i).map_or(self.src.len(), |t| t.end as usize)
    }
}

/// A string literal any MariaDB lexer reads back as `s`.
#[must_use]
pub fn sql_string(s: &str, no_backslash_escapes: bool) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' if !no_backslash_escapes => out.push_str("\\\\"),
            '\0' if !no_backslash_escapes => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// An identifier in backquotes.
#[must_use]
pub fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_round_trip() {
        let t = "ab\ncé d\n\nx";
        let p = Positions::new(t);
        for off in [0, 1, 3, 4, 6, 7, 9, 10] {
            assert_eq!(p.offset(p.location(off)), Some(off), "{off}");
        }
        assert_eq!(p.offset(Location { line: 2, column: 3 }), Some(6));
    }

    #[test]
    fn edits_apply_and_refuse_overlap() {
        let e = vec![
            Edit {
                start: 4,
                end: 4,
                with: "X".into(),
            },
            Edit {
                start: 0,
                end: 1,
                with: "a".into(),
            },
        ];
        assert_eq!(apply("A bcdef", &e).unwrap(), "a bcXdef");
        let bad = vec![
            Edit {
                start: 0,
                end: 3,
                with: String::new(),
            },
            Edit {
                start: 2,
                end: 4,
                with: String::new(),
            },
        ];
        assert!(apply("abcdef", &bad).is_err());
    }

    #[test]
    fn strings_quote_for_both_modes() {
        assert_eq!(sql_string("it's a\\b", false), "'it''s a\\\\b'");
        assert_eq!(sql_string("it's a\\b", true), "'it''s a\\b'");
        assert_eq!(quote("a`b"), "`a``b`");
    }
}
