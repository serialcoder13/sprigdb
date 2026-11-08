//! The persona-aware SQL lexer (chapter 07, "Lexing per persona"; chapter
//! 28, build step 1).
//!
//! [`lex`] splits statement text into [`Token`]s that cover every byte of
//! the input, in order, with no gaps: whitespace and comments are tokens
//! too, so span edits can rewrite one token and leave every other byte as
//! the client sent it. The lexer works on bytes, not `str`, because a
//! `COM_QUERY` payload may hold binary string literals; it assumes an
//! ASCII-compatible client character set (see [`charset_is_lexer_safe`]).
//!
//! What follows the persona and `sql_mode`:
//!
//! - **Executable comments.** `/*!NNNNN body */` runs `body` when `NNNNN`
//!   (five or six digits, or none) is at most the persona's version. On
//!   MariaDB, versions 50700 to 99999 are MySQL-only and never run, and
//!   `/*M!NNNNNN body */` runs by the same version rule; on MySQL `/*M!` is
//!   a plain comment. A running comment yields [`TokenKind::ExecOpen`], the
//!   body's tokens and [`TokenKind::ExecClose`]; a skipped one is a single
//!   [`TokenKind::InactiveExec`] token (see [`versioned`]).
//! - **Hints** `/*+ ... */` are one [`TokenKind::Hint`] token, kept as is.
//! - **`--` comments** need a space, a control character or the end of the
//!   input after the dashes; otherwise `--` is two minus signs. `#` starts a
//!   comment to the end of the line.
//! - **Quotes.** Backticks always quote identifiers. Double quotes quote
//!   identifiers under `ANSI_QUOTES` and strings otherwise. A quote doubled
//!   inside a quoted token stands for itself.
//! - **Backslash escapes** in strings, unless `NO_BACKSLASH_ESCAPES`.
//! - **Literals:** `N'..'`, `X'..'`, `B'..'`, `0x..`, `0b..`, decimal and
//!   approximate numbers. A `_charset` introducer is an ordinary word before
//!   a string.
//! - **Identifiers** may hold any character from U+0080 up, may start with
//!   a digit unless they are all digits, and after `ident.` a word starting
//!   with a digit is an identifier (`t.1col`).
//!
//! `DELIMITER` is a client command and never reaches the server.

pub mod versioned;

use sprig_proto_mysql::packets::Persona;

/// How to lex: the persona, its reported version and the `sql_mode` flags
/// that change tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexOptions {
    /// The persona the client talks to.
    pub persona: Persona,
    /// The persona's version as a number, `Mmmrr` or `MMmmrr`: 80406 for
    /// MySQL 8.4.6, 110803 for MariaDB 11.8.3. Gates executable comments.
    pub version: u32,
    /// `ANSI_QUOTES` is in `sql_mode`: `"x"` is an identifier.
    pub ansi_quotes: bool,
    /// `NO_BACKSLASH_ESCAPES` is in `sql_mode`: `\` is an ordinary
    /// character in strings.
    pub no_backslash_escapes: bool,
}

impl LexOptions {
    /// MySQL 8.4.6 with the default `sql_mode`.
    #[must_use]
    pub const fn mysql84() -> Self {
        Self {
            persona: Persona::MySql84,
            version: 80406,
            ansi_quotes: false,
            no_backslash_escapes: false,
        }
    }

    /// MariaDB 11.8.3 with the default `sql_mode`.
    #[must_use]
    pub const fn mariadb() -> Self {
        Self {
            persona: Persona::MariaDb,
            version: 110_803,
            ansi_quotes: false,
            no_backslash_escapes: false,
        }
    }

    /// The default options for a persona.
    #[must_use]
    pub const fn for_persona(persona: Persona) -> Self {
        match persona {
            Persona::MySql84 => Self::mysql84(),
            Persona::MariaDb => Self::mariadb(),
        }
    }

    /// With `ANSI_QUOTES` on or off.
    #[must_use]
    pub const fn ansi_quotes(mut self, on: bool) -> Self {
        self.ansi_quotes = on;
        self
    }

    /// With `NO_BACKSLASH_ESCAPES` on or off.
    #[must_use]
    pub const fn no_backslash_escapes(mut self, on: bool) -> Self {
        self.no_backslash_escapes = on;
        self
    }

    /// With another version number.
    #[must_use]
    pub const fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }
}

/// What a token is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenKind {
    /// Spaces, tabs and line breaks.
    Whitespace,
    /// `-- ...` or `# ...`, up to and including the line break.
    LineComment,
    /// `/* ... */`.
    BlockComment,
    /// An optimizer hint, `/*+ ... */`.
    Hint,
    /// An executable comment the persona skips: a comment.
    InactiveExec,
    /// The opening marker of an executable comment the persona runs, such
    /// as `/*!50001` or `/*M!100100`; the body's tokens follow.
    ExecOpen,
    /// The `*/` closing a running executable comment.
    ExecClose,
    /// An unquoted word: a keyword or an identifier.
    Word,
    /// A quoted identifier: `` `x` ``, or `"x"` under `ANSI_QUOTES`.
    QuotedIdent,
    /// A string literal: `'x'`, or `"x"` without `ANSI_QUOTES`.
    String,
    /// A national string, `N'x'`.
    NationalString,
    /// A hexadecimal string, `X'0A'`.
    HexString,
    /// A bit string, `B'01'`.
    BitString,
    /// A decimal or approximate number: `1`, `1.5`, `.5`, `1e-3`.
    Number,
    /// `0x0A`.
    HexNumber,
    /// `0b01`.
    BitNumber,
    /// A user variable: `@name`, `` @`name` ``, `@'name'`, `@"name"`.
    UserVar,
    /// A system variable: `@@name`, `@@session.name`.
    SysVar,
    /// A placeholder, `?`.
    Param,
    /// One of `. , ; ( )`.
    Punct,
    /// Any other operator: `=`, `:=`, `->>`, `<=>`, `*`, `:`, ...
    Op,
    /// A quoted token or comment with no end before the input does.
    Unterminated,
    /// A byte that starts no token (a stray control character).
    Other,
}

impl TokenKind {
    /// Whitespace, comments, hints and executable-comment markers: bytes
    /// with no meaning for the statement's structure.
    #[must_use]
    pub const fn is_trivia(self) -> bool {
        matches!(
            self,
            TokenKind::Whitespace
                | TokenKind::LineComment
                | TokenKind::BlockComment
                | TokenKind::Hint
                | TokenKind::InactiveExec
                | TokenKind::ExecOpen
                | TokenKind::ExecClose
        )
    }

    /// A word or a quoted identifier.
    #[must_use]
    pub const fn is_ident_like(self) -> bool {
        matches!(self, TokenKind::Word | TokenKind::QuotedIdent)
    }
}

/// One token: a kind and its byte range in the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    /// What it is.
    pub kind: TokenKind,
    /// First byte.
    pub start: u32,
    /// One past the last byte.
    pub end: u32,
}

impl Token {
    /// The token's bytes in `src`.
    #[must_use]
    pub fn bytes<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        &src[self.start as usize..self.end as usize]
    }

    /// The byte range.
    #[must_use]
    pub const fn range(&self) -> std::ops::Range<usize> {
        self.start as usize..self.end as usize
    }
}

/// Error returned for statements longer than the lexer's 4 GiB offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("statement of {0} bytes is too long to lex")]
pub struct TooLong(pub usize);

/// Splits `src` into tokens covering every byte, in order.
///
/// # Errors
///
/// [`TooLong`] when `src` is 4 GiB or more (the wire protocol's packet
/// limits keep real statements far below that).
pub fn lex(src: &[u8], opts: &LexOptions) -> Result<Vec<Token>, TooLong> {
    if u32::try_from(src.len()).is_err() {
        return Err(TooLong(src.len()));
    }
    let mut lx = Lexer {
        src,
        pos: 0,
        opts,
        toks: Vec::with_capacity(src.len() / 3 + 4),
        in_exec: false,
    };
    lx.run();
    Ok(lx.toks)
}

/// Whether a client character set keeps every byte below 0x80 meaning its
/// ASCII character, so the lexer can find quotes and backslashes by byte.
/// Not so for `big5`, `cp932`, `gbk`, `gb18030` and `sjis`, whose
/// multibyte characters may end in `\` or `'`: the gateway converts such
/// statements to `utf8mb4` before lexing, or refuses them.
#[must_use]
pub fn charset_is_lexer_safe(charset: &str) -> bool {
    !["big5", "cp932", "gbk", "gb18030", "sjis"]
        .iter()
        .any(|c| c.eq_ignore_ascii_case(charset))
}

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    opts: &'a LexOptions,
    toks: Vec<Token>,
    in_exec: bool,
}

/// Whether `b` may appear in an unquoted identifier: ASCII letters, digits,
/// `_`, `$`, and every byte of a multibyte UTF-8 character.
pub(crate) const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

impl Lexer<'_> {
    fn peek(&self, n: usize) -> Option<u8> {
        self.src.get(self.pos + n).copied()
    }

    #[allow(clippy::cast_possible_truncation)] // checked in `lex`
    fn push(&mut self, kind: TokenKind, start: usize) {
        self.toks.push(Token {
            kind,
            start: start as u32,
            end: self.pos as u32,
        });
    }

    fn run(&mut self) {
        while self.pos < self.src.len() {
            let start = self.pos;
            let kind = self.next_kind();
            debug_assert!(self.pos > start, "the lexer must always advance");
            self.push(kind, start);
        }
    }

    /// Whether the previous significant token is `.` directly after an
    /// identifier, so a word starting with a digit is an identifier.
    fn after_qualifier_dot(&self) -> bool {
        let mut sig = self.toks.iter().rev().filter(|t| !t.kind.is_trivia());
        let Some(dot) = sig.next() else { return false };
        if dot.kind != TokenKind::Punct || self.src[dot.start as usize] != b'.' {
            return false;
        }
        sig.next().is_some_and(|t| t.kind.is_ident_like())
    }

    #[allow(clippy::too_many_lines)]
    fn next_kind(&mut self) -> TokenKind {
        let b = self.src[self.pos];
        match b {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => {
                while matches!(
                    self.peek(0),
                    Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
                ) {
                    self.pos += 1;
                }
                TokenKind::Whitespace
            }
            b'#' => self.line_comment(),
            b'-' if self.peek(1) == Some(b'-') && self.peek(2).is_none_or(|c| c <= b' ') => {
                self.line_comment()
            }
            b'/' if self.peek(1) == Some(b'*') => self.block_comment(),
            b'*' if self.in_exec && self.peek(1) == Some(b'/') => {
                self.pos += 2;
                self.in_exec = false;
                TokenKind::ExecClose
            }
            b'`' => self.quoted(b'`', TokenKind::QuotedIdent, false),
            b'\'' => self.quoted(b'\'', TokenKind::String, !self.opts.no_backslash_escapes),
            b'"' => {
                if self.opts.ansi_quotes {
                    self.quoted(b'"', TokenKind::QuotedIdent, false)
                } else {
                    self.quoted(b'"', TokenKind::String, !self.opts.no_backslash_escapes)
                }
            }
            b'@' => self.variable(),
            b'?' => {
                self.pos += 1;
                TokenKind::Param
            }
            b'.' if self.peek(1).is_some_and(|c| c.is_ascii_digit()) && !self.after_ident_now() => {
                self.pos += 1;
                self.digits();
                self.exponent();
                TokenKind::Number
            }
            b'.' | b',' | b';' | b'(' | b')' => {
                self.pos += 1;
                TokenKind::Punct
            }
            b'N' | b'n' if self.peek(1) == Some(b'\'') => {
                self.pos += 1;
                self.quoted(
                    b'\'',
                    TokenKind::NationalString,
                    !self.opts.no_backslash_escapes,
                )
            }
            b'X' | b'x' if self.peek(1) == Some(b'\'') => {
                self.pos += 1;
                self.quoted(b'\'', TokenKind::HexString, false)
            }
            b'B' | b'b' if self.peek(1) == Some(b'\'') => {
                self.pos += 1;
                self.quoted(b'\'', TokenKind::BitString, false)
            }
            b'0'..=b'9' => self.number_or_word(),
            _ if is_ident_byte(b) => {
                while self.peek(0).is_some_and(is_ident_byte) {
                    self.pos += 1;
                }
                TokenKind::Word
            }
            _ => self.operator(),
        }
    }

    /// Whether the token just before this position (with no trivia between)
    /// is an identifier, so `.5` after `t` is `.` then `5`.
    fn after_ident_now(&self) -> bool {
        self.toks
            .last()
            .is_some_and(|t| t.kind.is_ident_like() && t.end as usize == self.pos)
    }

    fn line_comment(&mut self) -> TokenKind {
        while let Some(c) = self.peek(0) {
            self.pos += 1;
            if c == b'\n' {
                break;
            }
        }
        TokenKind::LineComment
    }

    fn block_comment(&mut self) -> TokenKind {
        let start = self.pos;
        let (kind, body_start) = match (self.peek(2), self.peek(3)) {
            (Some(b'+'), _) => (TokenKind::Hint, start + 3),
            (Some(b'!'), _) | (Some(b'M'), Some(b'!')) if !self.in_exec => {
                let m = versioned::ExecMarker::parse(&self.src[start..])
                    .expect("a /*! or /*M! prefix always parses");
                if m.runs_on(self.opts.persona, self.opts.version) {
                    self.pos = start + m.len;
                    self.in_exec = true;
                    return TokenKind::ExecOpen;
                }
                (TokenKind::InactiveExec, start + m.len)
            }
            _ => (TokenKind::BlockComment, start + 2),
        };
        self.pos = body_start;
        match find(&self.src[self.pos..], b"*/") {
            Some(i) => {
                self.pos += i + 2;
                kind
            }
            None => {
                self.pos = self.src.len();
                TokenKind::Unterminated
            }
        }
    }

    fn quoted(&mut self, q: u8, kind: TokenKind, backslash: bool) -> TokenKind {
        self.pos += 1;
        while let Some(c) = self.peek(0) {
            if backslash && c == b'\\' {
                self.pos = (self.pos + 2).min(self.src.len());
                continue;
            }
            self.pos += 1;
            if c == q {
                if self.peek(0) == Some(q) {
                    self.pos += 1;
                    continue;
                }
                return kind;
            }
        }
        TokenKind::Unterminated
    }

    fn variable(&mut self) -> TokenKind {
        self.pos += 1;
        if self.peek(0) == Some(b'@') {
            self.pos += 1;
            // @@name, @@session.name, @@`name`: a dotted run of words.
            loop {
                match self.peek(0) {
                    Some(b'`') => {
                        if self.quoted(b'`', TokenKind::SysVar, false) == TokenKind::Unterminated {
                            return TokenKind::Unterminated;
                        }
                    }
                    Some(c) if is_ident_byte(c) => {
                        while self.peek(0).is_some_and(is_ident_byte) {
                            self.pos += 1;
                        }
                    }
                    _ => break,
                }
                if self.peek(0) == Some(b'.')
                    && self.peek(1).is_some_and(|c| c == b'`' || is_ident_byte(c))
                {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            return TokenKind::SysVar;
        }
        match self.peek(0) {
            Some(q @ (b'`' | b'\'' | b'"')) => {
                let backslash = q != b'`' && !self.opts.no_backslash_escapes;
                if self.quoted(q, TokenKind::UserVar, backslash) == TokenKind::Unterminated {
                    TokenKind::Unterminated
                } else {
                    TokenKind::UserVar
                }
            }
            _ => {
                // User variable names may hold `.`, `_` and `$`.
                while self.peek(0).is_some_and(|c| is_ident_byte(c) || c == b'.') {
                    self.pos += 1;
                }
                TokenKind::UserVar
            }
        }
    }

    fn digits(&mut self) {
        while self.peek(0).is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    /// An optional exponent: `e`, an optional sign, digits.
    fn exponent(&mut self) {
        if matches!(self.peek(0), Some(b'e' | b'E')) {
            let sign = usize::from(matches!(self.peek(1), Some(b'+' | b'-')));
            if self.peek(1 + sign).is_some_and(|c| c.is_ascii_digit()) {
                self.pos += 1 + sign;
                self.digits();
            }
        }
    }

    /// At a digit: a number, `0x..`, `0b..`, or an identifier that starts
    /// with digits (`1abc`, `0xZZ`).
    fn number_or_word(&mut self) -> TokenKind {
        let start = self.pos;
        let mut end = start;
        while self.src.get(end).copied().is_some_and(is_ident_byte) {
            end += 1;
        }
        let word = &self.src[start..end];
        if self.after_qualifier_dot() {
            self.pos = end;
            return TokenKind::Word;
        }
        if word.len() > 2
            && word[0] == b'0'
            && word[1] == b'x'
            && word[2..].iter().all(u8::is_ascii_hexdigit)
        {
            self.pos = end;
            return TokenKind::HexNumber;
        }
        if word.len() > 2
            && word[0] == b'0'
            && word[1] == b'b'
            && word[2..].iter().all(|c| matches!(c, b'0' | b'1'))
        {
            self.pos = end;
            return TokenKind::BitNumber;
        }
        let ndig = word.iter().take_while(|c| c.is_ascii_digit()).count();
        if ndig == word.len() {
            // Digits, then maybe a fraction and an exponent.
            self.pos = end;
            if self.peek(0) == Some(b'.') {
                self.pos += 1;
                self.digits();
            }
            self.exponent();
            if self.peek(0).is_some_and(is_ident_byte) {
                // `1.5abc` and the like: let the word run on as MySQL's
                // lexer does not; keep the number and lex the rest apart.
            }
            return TokenKind::Number;
        }
        // `1e5`, `1e+5`, `1e-5` are numbers; `1e`, `1ea`, `12abc` words.
        let rest = &word[ndig..];
        if matches!(rest[0], b'e' | b'E') {
            if rest.len() > 1 && rest[1..].iter().all(u8::is_ascii_digit) {
                self.pos = end;
                return TokenKind::Number;
            }
            if rest.len() == 1
                && matches!(self.src.get(end), Some(b'+' | b'-'))
                && self.src.get(end + 1).is_some_and(u8::is_ascii_digit)
            {
                self.pos = end + 1;
                self.digits();
                return TokenKind::Number;
            }
        }
        self.pos = end;
        TokenKind::Word
    }

    fn operator(&mut self) -> TokenKind {
        const THREE: [&[u8]; 2] = [b"<=>", b"->>"];
        const TWO: [&[u8]; 10] = [
            b":=", b"<=", b">=", b"<>", b"!=", b"&&", b"||", b"<<", b">>", b"->",
        ];
        let rest = &self.src[self.pos..];
        let len = if THREE.iter().any(|o| rest.starts_with(o)) {
            3
        } else if TWO.iter().any(|o| rest.starts_with(o)) {
            2
        } else {
            1
        };
        let b = rest[0];
        self.pos += len;
        if len == 1 && !b"=<>!~^&|+-*/%:{}[]\\".contains(&b) {
            return TokenKind::Other;
        }
        TokenKind::Op
    }
}

/// The first position of `needle` in `hay`.
pub(crate) fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// The value of a quoted identifier or a word: quotes removed and doubled
/// quotes undone. Returns the bytes unchanged for a word.
#[must_use]
pub fn ident_value(tok: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    use std::borrow::Cow;
    match tok.first() {
        Some(&q @ (b'`' | b'"')) if tok.len() >= 2 && tok[tok.len() - 1] == q => {
            let inner = &tok[1..tok.len() - 1];
            if inner.contains(&q) {
                let mut v = Vec::with_capacity(inner.len());
                let mut i = 0;
                while i < inner.len() {
                    v.push(inner[i]);
                    i += if inner[i] == q && inner.get(i + 1) == Some(&q) {
                        2
                    } else {
                        1
                    };
                }
                Cow::Owned(v)
            } else {
                Cow::Borrowed(inner)
            }
        }
        _ => Cow::Borrowed(tok),
    }
}

/// The value of a string literal token (`'..'` or `"..."`), with escapes
/// and doubled quotes undone. `None` if the token is not a complete
/// string.
#[must_use]
pub fn string_value(tok: &[u8], no_backslash_escapes: bool) -> Option<Vec<u8>> {
    let q = *tok.first()?;
    if !matches!(q, b'\'' | b'"') || tok.len() < 2 || tok[tok.len() - 1] != q {
        return None;
    }
    let inner = &tok[1..tok.len() - 1];
    let mut v = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if c == b'\\' && !no_backslash_escapes && i + 1 < inner.len() {
            let e = inner[i + 1];
            // `\%` and `\_` keep the backslash (LIKE patterns).
            if matches!(e, b'%' | b'_') {
                v.push(b'\\');
            }
            v.push(match e {
                b'0' => 0,
                b'b' => 8,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'Z' => 26,
                _ => e,
            });
            i += 2;
        } else if c == q && inner.get(i + 1) == Some(&q) {
            v.push(q);
            i += 2;
        } else {
            v.push(c);
            i += 1;
        }
    }
    Some(v)
}

#[cfg(test)]
mod tests;
