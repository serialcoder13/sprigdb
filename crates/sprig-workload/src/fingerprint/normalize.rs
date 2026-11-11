//! The SQL normalizer: one statement's tokens to its normalized text,
//! literal list and table references.

use sprig_sql::lex::{self, LexOptions, Token, TokenKind, ident_value};

use super::keywords::{
    CLAUSE_END, JSON_PATH_FUNC, KEYWORD, Kw, NOT_ALIAS, RESERVED, RESERVED_FUNC, TABLE_MODIFIER,
    WordInfo, classify, eq_kw,
};
use super::{Literal, LiteralKind};
use crate::shape::StmtType;

/// A table reference the normalizer found, with logical names as written
/// (quotes removed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    /// The qualifier as written, else the session's current database;
    /// `None` for CTE names and when there is neither.
    pub db: Option<String>,
    /// The table name.
    pub name: String,
    /// The alias, when the statement gives one.
    pub alias: Option<String>,
    /// The name refers to a CTE of the statement, not a table.
    pub cte: bool,
}

/// One normalized statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Normalized {
    /// The whole normalized text (not yet cut at `workload.max_text_bytes`).
    pub text: String,
    /// The literals, in text order, each with its slot.
    pub literals: Vec<Literal>,
    /// How many slots the text has (`?` and collapsed `(...)`).
    pub slots: u32,
    /// The slots that are collapsed lists (`IN (...)`, `VALUES (...)`),
    /// ascending.
    pub list_slots: Vec<u32>,
    /// The table references, in text order.
    pub tables: Vec<TableName>,
    /// The statement type, from its first keyword.
    pub stmt_type: StmtType,
    /// The statement carries a password: `IDENTIFIED BY`, `PASSWORD`,
    /// `MASTER_PASSWORD`, or credentials (`CREDENTIALS (...)`). Its
    /// literals are already dropped.
    pub sensitive: bool,
    /// The statement couldn't be lexed whole (an unterminated string,
    /// identifier or comment, or more than 4 GiB): count it under the
    /// reserved "unparsed" fingerprint.
    pub unparsed: bool,
    /// The longest collapsed `IN` list, in elements.
    pub in_list_max: u32,
}

impl Normalized {
    /// The text cut to at most `max` bytes on a character boundary, and
    /// whether it was cut.
    #[must_use]
    pub fn stored_text(&self, max: usize) -> (&str, bool) {
        if self.text.len() <= max {
            return (&self.text, false);
        }
        let mut end = max;
        while end > 0 && !self.text.is_char_boundary(end) {
            end -= 1;
        }
        (&self.text[..end], true)
    }

    fn lex_failed() -> Self {
        Self {
            text: String::new(),
            literals: Vec::new(),
            slots: 0,
            list_slots: Vec::new(),
            tables: Vec::new(),
            stmt_type: StmtType::Other,
            sensitive: false,
            unparsed: true,
            in_list_max: 0,
        }
    }
}

/// Normalizes one statement (chapter 12, "SQL fingerprints"). `sql` is the
/// statement's text as the client sent it (one statement; a trailing `;`
/// is ignored), `current_db` the session's logical database. Never fails:
/// a statement that can't be lexed comes back with
/// [`Normalized::unparsed`] set.
#[must_use]
#[allow(clippy::trivially_copy_pass_by_ref)] // LexOptions may grow
pub fn normalize(
    sql: &[u8],
    lex_opts: &LexOptions,
    current_db: Option<&str>,
    lower_case_table_names: bool,
) -> Normalized {
    normalize_with(sql, lex_opts, current_db, lower_case_table_names, true)
}

/// [`normalize`], with the table names only when `want_tables` (they
/// allocate; the hot path doesn't need them).
#[allow(clippy::trivially_copy_pass_by_ref)] // LexOptions may grow
pub(crate) fn normalize_with(
    sql: &[u8],
    lex_opts: &LexOptions,
    current_db: Option<&str>,
    lower_case_table_names: bool,
    want_tables: bool,
) -> Normalized {
    let Ok(tokens) = lex::lex(sql, lex_opts) else {
        return Normalized::lex_failed();
    };
    let sig = significant(sql, &tokens, lex_opts);
    drop(tokens);
    let mut w = Walker {
        src: sql,
        sig: &sig,
        outs: Vec::with_capacity(sig.len()),
        lits: Vec::new(),
        tables: Vec::new(),
        depth: 0,
        parens: Vec::new(),
        list_ctx: 0,
        expect_table: false,
        alias_pending: false,
        after_as_alias: false,
        cte_names: Vec::new(),
        cte_depth: None,
        cte_next: false,
        saw_rename: false,
        first: (0, 0),
        no_backslash_escapes: lex_opts.no_backslash_escapes,
        stmt_type: StmtType::Other,
        first_word: true,
        sensitive: false,
        unparsed: false,
        params: 0,
    };
    w.run();
    let Walker {
        outs,
        mut lits,
        tables,
        stmt_type,
        sensitive,
        unparsed,
        ..
    } = w;
    let outs = collapse(sql, outs);
    let r = render(sql, &outs, &mut lits, current_db, lower_case_table_names);
    let tables = if want_tables {
        tables
            .into_iter()
            .map(|t| t.finish(sql, current_db, lower_case_table_names))
            .collect()
    } else {
        Vec::new()
    };
    Normalized {
        text: r.text,
        literals: if sensitive { Vec::new() } else { lits },
        slots: r.slots,
        list_slots: r.list_slots,
        tables,
        stmt_type,
        sensitive,
        unparsed,
        in_list_max: r.in_list_max,
    }
}

/// A significant token: the lexer's token, or a hint marker.
#[derive(Debug, Clone, Copy)]
struct Sig {
    kind: TokenKind,
    start: u32,
    end: u32,
    /// 1: the `/*+` of a hint, 2: its `*/`; 0 otherwise.
    marker: u8,
    /// Inside a hint.
    hint: bool,
}

/// The significant tokens: trivia dropped, running executable comments
/// flattened into their content, hints expanded into their lexed body
/// between markers.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn significant(src: &[u8], tokens: &[Token], opts: &LexOptions) -> Vec<Sig> {
    let mut out = Vec::with_capacity(tokens.len());
    for t in tokens {
        match t.kind {
            TokenKind::Hint => {
                let body = (t.start as usize + 3)..(t.end as usize).saturating_sub(2);
                out.push(Sig {
                    kind: t.kind,
                    start: t.start,
                    end: t.start + 3,
                    marker: 1,
                    hint: true,
                });
                if body.start <= body.end
                    && let Ok(inner) = lex::lex(&src[body.clone()], opts)
                {
                    let base = u32::try_from(body.start).unwrap_or(0);
                    for it in inner.iter().filter(|it| !it.kind.is_trivia()) {
                        out.push(Sig {
                            kind: it.kind,
                            start: it.start + base,
                            end: it.end + base,
                            marker: 0,
                            hint: true,
                        });
                    }
                }
                out.push(Sig {
                    kind: t.kind,
                    start: t.end.saturating_sub(2),
                    end: t.end,
                    marker: 2,
                    hint: true,
                });
            }
            k if k.is_trivia() => {}
            k => out.push(Sig {
                kind: k,
                start: t.start,
                end: t.end,
                marker: 0,
                hint: false,
            }),
        }
    }
    out
}

/// A byte range of the statement.
type Src = (u32, u32);

/// One output token, before rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Out {
    /// A keyword: upper case. `null_or_end`: `NULL` or `END`, which end an
    /// operand.
    Kw(Src, Kw),
    /// A function name: upper case, no space before its `(`.
    Func(Src),
    /// A column or other identifier: lower case, unquoted.
    Ident(Src),
    /// An alias or qualifier: as written, unquoted.
    Name(Src),
    /// A table reference: `db.table`.
    Table {
        db: Option<Src>,
        name: Src,
        cte: bool,
    },
    /// One literal: `?`. The index into the literal list.
    Lit(u32),
    /// A `LIKE` pattern literal with a leading wildcard (`'%x'`, `'_x'`):
    /// `%?`, so the two access paths are different fingerprints.
    WildLit(u32),
    /// A collapsed list: `(...)`. Literal indexes `first..end`, and the
    /// number of elements when it was an `IN` list (0 otherwise).
    Collapsed { first: u32, end: u32, in_elems: u32 },
    /// `( ) , . ;`
    Punct(u8),
    /// An operator, as written.
    Op(Src),
    /// A variable, lower case.
    Var(Src),
    /// Fixed text.
    Static(&'static str),
    /// Bytes as written.
    Raw(Src),
}

impl Out {
    /// Whether the token ends an operand, so a following `-` is binary.
    fn ends_operand(self) -> bool {
        match self {
            Out::Ident(_)
            | Out::Name(_)
            | Out::Table { .. }
            | Out::Lit(_)
            | Out::WildLit(_)
            | Out::Collapsed { .. }
            | Out::Var(_) => true,
            Out::Punct(c) => c == b')',
            Out::Kw(_, k) => matches!(k, Kw::Null | Kw::End),
            _ => false,
        }
    }

    fn is_kw(self, k: Kw) -> bool {
        matches!(self, Out::Kw(_, x) if x == k)
    }
}

fn bytes(src: &[u8], s: Src) -> &[u8] {
    src.get(s.0 as usize..s.1 as usize).unwrap_or_default()
}

/// A table reference while walking.
struct RawTable {
    db: Option<Src>,
    name: Src,
    alias: Option<Src>,
    cte: bool,
}

impl RawTable {
    fn finish(self, src: &[u8], current_db: Option<&str>, ltn: bool) -> TableName {
        let text = |s: Src| {
            let v = String::from_utf8_lossy(&ident_value(bytes(src, s))).into_owned();
            if ltn { v.to_lowercase() } else { v }
        };
        let db = if self.cte {
            None
        } else {
            self.db
                .map(text)
                .or_else(|| current_db.map(|d| if ltn { d.to_lowercase() } else { d.to_owned() }))
        };
        TableName {
            db,
            name: text(self.name),
            alias: self.alias.map(text),
            cte: self.cte,
        }
    }
}

/// The token walk.
#[allow(clippy::struct_excessive_bools)]
struct Walker<'a> {
    src: &'a [u8],
    sig: &'a [Sig],
    outs: Vec<Out>,
    lits: Vec<Literal>,
    tables: Vec<RawTable>,
    depth: u32,
    /// Per open parenthesis: the function it calls, if it is a JSON path
    /// function, and the index of the current argument.
    parens: Vec<(bool, bool, u32)>,
    /// Bit `d` set: commas separate table references at depth `d` (depths
    /// past 63 never do).
    list_ctx: u64,
    expect_table: bool,
    /// The previous token was a table reference: an alias may follow.
    alias_pending: bool,
    /// The previous token was `AS` after a table reference.
    after_as_alias: bool,
    cte_names: Vec<Src>,
    cte_depth: Option<u32>,
    cte_next: bool,
    saw_rename: bool,
    /// The statement's first word.
    first: Src,
    no_backslash_escapes: bool,
    stmt_type: StmtType,
    first_word: bool,
    sensitive: bool,
    unparsed: bool,
    params: u32,
}

#[allow(clippy::many_single_char_names)] // t: token, s: span, w: word, i: index
impl Walker<'_> {
    fn sig_is(&self, i: usize, b: u8) -> bool {
        self.sig.get(i).is_some_and(|t| {
            t.kind == TokenKind::Punct
                && t.marker == 0
                && t.end == t.start + 1
                && self.src.get(t.start as usize) == Some(&b)
        })
    }

    fn sig_word(&self, i: usize) -> Option<&[u8]> {
        self.sig
            .get(i)
            .filter(|t| t.kind == TokenKind::Word && t.marker == 0)
            .map(|t| bytes(self.src, (t.start, t.end)))
    }

    fn push_lit(&mut self, kind: LiteralKind, start: u32, end: u32) {
        let param_index = if kind == LiteralKind::Param {
            self.params += 1;
            self.params - 1
        } else {
            0
        };
        let idx = u32::try_from(self.lits.len()).unwrap_or(u32::MAX);
        self.lits.push(Literal {
            kind,
            span: start..end,
            slot: 0,
            param_index,
        });
        self.outs.push(Out::Lit(idx));
    }

    fn last_ends_operand(&self) -> bool {
        self.outs.last().is_some_and(|o| o.ends_operand())
    }

    fn set_list_ctx(&mut self, on: bool) {
        if self.depth < 64 {
            let bit = 1u64 << self.depth;
            if on {
                self.list_ctx |= bit;
            } else {
                self.list_ctx &= !bit;
            }
        }
    }

    fn list_ctx(&self) -> bool {
        self.depth < 64 && self.list_ctx & (1u64 << self.depth) != 0
    }

    fn in_func_paren(&self) -> bool {
        self.parens.last().is_some_and(|p| p.0)
    }

    /// Whether a string here is a JSON path argument (kept in the text:
    /// paths are structure, not values): the right side of `->` or `->>`,
    /// or the second or later argument of the JSON functions that take a
    /// path.
    fn json_path_position(&self) -> bool {
        if let Some(Out::Op(o)) = self.outs.last() {
            let b = bytes(self.src, *o);
            if b == b"->" || b == b"->>" {
                return true;
            }
        }
        self.parens.last().is_some_and(|p| p.1 && p.2 >= 1)
    }

    #[allow(clippy::too_many_lines)]
    fn run(&mut self) {
        let mut i = 0;
        while i < self.sig.len() {
            let t = self.sig[i];
            let s = (t.start, t.end);
            if t.marker != 0 {
                self.outs
                    .push(Out::Static(if t.marker == 1 { "/*+" } else { "*/" }));
                i += 1;
                continue;
            }
            if t.hint {
                // Inside a hint: normalized like other tokens, without
                // touching the statement's table positions.
                i = self.hint_token(i);
                continue;
            }
            let was_alias_pending = std::mem::take(&mut self.alias_pending);
            let was_after_as = std::mem::take(&mut self.after_as_alias);
            match t.kind {
                TokenKind::Number | TokenKind::HexNumber | TokenKind::BitNumber => {
                    let kind = match t.kind {
                        TokenKind::HexNumber => LiteralKind::Hex,
                        TokenKind::BitNumber => LiteralKind::Bit,
                        _ => LiteralKind::Number,
                    };
                    self.push_lit(kind, t.start, t.end);
                    self.expect_table = false;
                }
                TokenKind::String
                | TokenKind::NationalString
                | TokenKind::HexString
                | TokenKind::BitString => {
                    self.string(i);
                }
                TokenKind::Param => {
                    self.push_lit(LiteralKind::Param, t.start, t.end);
                    self.expect_table = false;
                }
                TokenKind::UserVar | TokenKind::SysVar => {
                    self.outs.push(Out::Var(s));
                    self.expect_table = false;
                }
                TokenKind::Punct => self.punct(i),
                TokenKind::Op => {
                    let b = bytes(self.src, s);
                    let sign = b == b"-" || b == b"+";
                    let next_num = self.sig.get(i + 1).is_some_and(|n| {
                        n.marker == 0
                            && matches!(
                                n.kind,
                                TokenKind::Number | TokenKind::HexNumber | TokenKind::BitNumber
                            )
                    });
                    if sign && next_num && !self.last_ends_operand() {
                        let n = self.sig[i + 1];
                        self.push_lit(LiteralKind::Number, t.start, n.end);
                        i += 2;
                        continue;
                    }
                    // `LIKE %?`, as the normalizer writes a leading-wildcard
                    // pattern: read back as the same slot (idempotence).
                    let next_param = self
                        .sig
                        .get(i + 1)
                        .is_some_and(|n| n.marker == 0 && n.kind == TokenKind::Param);
                    if b == b"%"
                        && next_param
                        && self.outs.last().is_some_and(|o| o.is_kw(Kw::Like))
                    {
                        let n = self.sig[i + 1];
                        self.push_lit(LiteralKind::Param, n.start, n.end);
                        if let Some(Out::Lit(k)) = self.outs.pop() {
                            self.outs.push(Out::WildLit(k));
                        }
                        i += 2;
                        continue;
                    }
                    self.outs.push(Out::Op(s));
                    self.expect_table = false;
                }
                TokenKind::Word | TokenKind::QuotedIdent => {
                    i = self.word(i, was_alias_pending, was_after_as);
                    continue;
                }
                TokenKind::Unterminated => {
                    self.unparsed = true;
                    self.outs.push(Out::Raw(s));
                }
                _ => self.outs.push(Out::Raw(s)),
            }
            i += 1;
        }
    }

    /// One token inside an optimizer hint; returns the next index.
    fn hint_token(&mut self, i: usize) -> usize {
        let t = self.sig[i];
        let s = (t.start, t.end);
        match t.kind {
            TokenKind::Number
            | TokenKind::HexNumber
            | TokenKind::BitNumber
            | TokenKind::String
            | TokenKind::NationalString
            | TokenKind::HexString
            | TokenKind::BitString
            | TokenKind::Param => self.push_lit(LiteralKind::Number, t.start, t.end),
            TokenKind::Word | TokenKind::QuotedIdent => {
                let quoted = t.kind == TokenKind::QuotedIdent;
                let info = if quoted {
                    WordInfo::NONE
                } else {
                    classify(bytes(self.src, s))
                };
                let out = if self.sig_is(i + 1, b'(') && !quoted {
                    Out::Func(s)
                } else if info.has(KEYWORD) {
                    Out::Kw(s, info.kw)
                } else {
                    Out::Raw(s)
                };
                self.outs.push(out);
            }
            TokenKind::Punct => {
                let c = self.src.get(t.start as usize).copied().unwrap_or(b' ');
                self.outs.push(Out::Punct(c));
            }
            TokenKind::Unterminated => {
                self.unparsed = true;
                self.outs.push(Out::Raw(s));
            }
            TokenKind::Op => {
                let b = bytes(self.src, s);
                let num = self.sig.get(i + 1).is_some_and(|n| {
                    n.hint
                        && n.marker == 0
                        && matches!(
                            n.kind,
                            TokenKind::Number | TokenKind::HexNumber | TokenKind::BitNumber
                        )
                });
                if (b == b"-" || b == b"+") && num && !self.last_ends_operand() {
                    let n = self.sig[i + 1];
                    self.push_lit(LiteralKind::Number, t.start, n.end);
                    return i + 2;
                }
                self.outs.push(Out::Op(s));
            }
            _ => self.outs.push(Out::Raw(s)),
        }
        i + 1
    }

    fn string(&mut self, i: usize) {
        let t = self.sig[i];
        let s = (t.start, t.end);
        self.expect_table = false;
        // Adjacent strings are one literal: 'a' 'b'.
        let prev_string = i > 0
            && matches!(
                self.sig[i - 1].kind,
                TokenKind::String | TokenKind::NationalString
            )
            && self.sig[i - 1].marker == 0
            && t.kind == TokenKind::String;
        if prev_string
            && let Some(Out::Lit(k) | Out::WildLit(k)) = self.outs.last()
            && let Some(l) = self.lits.get_mut(*k as usize)
        {
            l.span.end = t.end;
            return;
        }
        let kind = match t.kind {
            TokenKind::HexString => LiteralKind::Hex,
            TokenKind::BitString => LiteralKind::Bit,
            _ => LiteralKind::String,
        };
        if kind == LiteralKind::String && self.json_path_position() {
            self.outs.push(Out::Raw(s));
            return;
        }
        let wild = kind == LiteralKind::String
            && self.outs.last().is_some_and(|o| o.is_kw(Kw::Like))
            && leading_wildcard(bytes(self.src, s), self.no_backslash_escapes);
        self.push_lit(kind, t.start, t.end);
        if wild && let Some(Out::Lit(k)) = self.outs.pop() {
            self.outs.push(Out::WildLit(k));
        }
    }

    fn punct(&mut self, i: usize) {
        let t = self.sig[i];
        match self.src.get(t.start as usize).copied().unwrap_or(b' ') {
            b'(' => {
                let (func, json) = match self.outs.last() {
                    Some(Out::Func(f)) => (true, classify(bytes(self.src, *f)).has(JSON_PATH_FUNC)),
                    _ => (false, false),
                };
                self.outs.push(Out::Punct(b'('));
                self.depth += 1;
                self.parens.push((func, json, 0));
                // `FROM (t1 JOIN t2, t3)`: a parenthesized table list.
                let tables = self.expect_table;
                self.set_list_ctx(tables);
                // `FROM (t1 JOIN t2)` keeps expecting a table.
            }
            b')' => {
                self.outs.push(Out::Punct(b')'));
                self.depth = self.depth.saturating_sub(1);
                self.parens.pop();
                self.expect_table = false;
                if self.cte_depth.is_some_and(|d| d > self.depth) {
                    self.cte_depth = None;
                }
            }
            b',' => {
                self.outs.push(Out::Punct(b','));
                if let Some(p) = self.parens.last_mut() {
                    p.2 += 1;
                }
                if self.list_ctx() {
                    self.expect_table = true;
                } else if self.cte_depth == Some(self.depth) {
                    self.cte_next = true;
                }
            }
            b';' if i + 1 == self.sig.len() => {}
            c => self.outs.push(Out::Punct(c)),
        }
    }

    /// Handles the word at `i`; returns the next index.
    #[allow(clippy::too_many_lines, clippy::fn_params_excessive_bools)]
    fn word(&mut self, i: usize, alias_pending: bool, after_as: bool) -> usize {
        let t = self.sig[i];
        let s = (t.start, t.end);
        let quoted = t.kind == TokenKind::QuotedIdent;
        let src = self.src;
        let w = bytes(src, s);
        let info = if quoted { WordInfo::NONE } else { classify(w) };
        let prev_dot = i > 0 && self.sig_is(i - 1, b'.');
        let next_dot = self.sig_is(i + 1, b'.');
        let next_paren = self.sig_is(i + 1, b'(');
        let next = self.sig.get(i + 1).copied();

        if !quoted
            && (info.kw == Kw::Identified
                || info.kw == Kw::None && w.len() >= 8 && contains_password(w)
                // Chapter 51's `CREDENTIALS (k = 'v', ...)` (bugfix-24).
                || info.kw == Kw::None && w.eq_ignore_ascii_case(b"CREDENTIALS"))
        {
            self.sensitive = true;
        }

        // Literals spelled as words.
        if !quoted && !prev_dot {
            if matches!(info.kw, Kw::True | Kw::False) {
                self.push_lit(LiteralKind::Bool, t.start, t.end);
                self.first_word = false;
                return i + 1;
            }
            let next_kind = next.filter(|n| n.marker == 0).map(|n| n.kind);
            if matches!(info.kw, Kw::Date | Kw::Time | Kw::Timestamp)
                && matches!(
                    next_kind,
                    Some(TokenKind::String | TokenKind::NationalString)
                )
            {
                let n = self.sig[i + 1];
                self.push_lit(LiteralKind::Temporal, t.start, n.end);
                return i + 2;
            }
            if w.len() > 1 && w[0] == b'_' {
                let kind = match next_kind {
                    Some(TokenKind::String) => Some(LiteralKind::String),
                    Some(TokenKind::HexString | TokenKind::HexNumber) => Some(LiteralKind::Hex),
                    Some(TokenKind::BitString | TokenKind::BitNumber) => Some(LiteralKind::Bit),
                    _ => None,
                };
                if let Some(kind) = kind {
                    let n = self.sig[i + 1];
                    self.push_lit(kind, t.start, n.end);
                    return i + 2;
                }
            }
        }

        if self.first_word {
            self.first_word = false;
            self.first = s;
            self.stmt_type = info.stmt.unwrap_or(StmtType::Other);
            let insert = (matches!(info.kw, Kw::Insert | Kw::Replace) || eq_kw(w, "TRUNCATE"))
                && !next_paren;
            if insert || matches!(info.kw, Kw::Describe | Kw::Desc | Kw::Explain) {
                self.outs.push(Out::Kw(s, info.kw));
                self.expect_table = true;
                return i + 1;
            }
            if info.kw == Kw::Rename {
                self.saw_rename = true;
            }
            if info.kw == Kw::With {
                self.outs.push(Out::Kw(s, info.kw));
                self.cte_depth = Some(self.depth);
                self.cte_next = true;
                self.stmt_type = with_stmt_type(src, &self.sig[i..]);
                return i + 1;
            }
        }

        // A CTE name.
        if self.cte_next && !prev_dot {
            if info.kw == Kw::Recursive {
                self.outs.push(Out::Kw(s, info.kw));
                return i + 1;
            }
            self.cte_next = false;
            if !info.has(KEYWORD) {
                self.cte_names.push(s);
                self.outs.push(Out::Name(s));
                return i + 1;
            }
        }

        // A table reference.
        if self.expect_table && !prev_dot {
            if info.has(TABLE_MODIFIER) {
                self.outs.push(Out::Kw(s, info.kw));
                return i + 1;
            }
            let assignment = self.sig.get(i + 1).is_some_and(|n| {
                n.kind == TokenKind::Op && bytes(self.src, (n.start, n.end)) == b"="
            });
            if !info.has(RESERVED) && !assignment {
                return self.table_ref(i);
            }
            self.expect_table = false;
        }

        // An alias after a table reference.
        if alias_pending && !prev_dot {
            if info.kw == Kw::As {
                self.outs.push(Out::Kw(s, info.kw));
                self.after_as_alias = true;
                return i + 1;
            }
            let ok = !info.has(KEYWORD) && !info.has(NOT_ALIAS);
            if ok && !next_dot && !next_paren {
                self.set_alias(s);
                self.outs.push(Out::Name(s));
                return i + 1;
            }
        }
        if after_as && !next_dot {
            self.set_alias(s);
            self.outs.push(Out::Name(s));
            return i + 1;
        }

        if prev_dot {
            let out = if next_dot {
                Out::Name(s)
            } else if next_paren {
                Out::Func(s)
            } else {
                Out::Ident(s)
            };
            self.outs.push(out);
            return i + 1;
        }
        if !info.has(KEYWORD) && !next_dot && self.outs.last().is_some_and(|o| o.is_kw(Kw::As)) {
            self.outs.push(Out::Name(s));
            self.expect_table = false;
            return i + 1;
        }
        if next_dot {
            self.outs.push(Out::Name(s));
            return i + 1;
        }
        let func =
            next_paren && info.kw != Kw::Value && (!info.has(RESERVED) || info.has(RESERVED_FUNC));
        if func {
            self.outs.push(Out::Func(s));
            return i + 1;
        }
        if !info.has(KEYWORD) {
            let after_as_kw = self.outs.last().is_some_and(|o| o.is_kw(Kw::As));
            self.outs.push(if after_as_kw {
                Out::Name(s)
            } else {
                Out::Ident(s)
            });
            self.expect_table = false;
            return i + 1;
        }

        // A keyword.
        self.keyword(i, info);
        self.outs.push(Out::Kw(s, info.kw));
        i + 1
    }

    /// The side effects of a keyword on table positions.
    fn keyword(&mut self, i: usize, info: WordInfo) {
        let first = bytes(self.src, self.first);
        let show = eq_kw(first, "SHOW");
        let ddl = self.stmt_type == StmtType::Ddl;
        match info.kw {
            Kw::From => {
                if !show && !self.in_func_paren() {
                    self.expect_table = true;
                    self.set_list_ctx(true);
                }
            }
            Kw::Join | Kw::StraightJoin => {
                if self.list_ctx() {
                    self.expect_table = true;
                }
            }
            Kw::Update => {
                let prev = if i > 0 { self.sig_word(i - 1) } else { None };
                let after = prev.is_some_and(|p| eq_kw(p, "FOR") || eq_kw(p, "KEY"));
                if !after {
                    self.expect_table = true;
                    self.set_list_ctx(true);
                }
            }
            Kw::Into => self.expect_table = true,
            Kw::Table | Kw::Tables => {
                if !show {
                    self.expect_table = true;
                    // Lists only in DROP TABLE a, b; RENAME TABLE; LOCK TABLES.
                    let list =
                        eq_kw(first, "DROP") || eq_kw(first, "RENAME") || eq_kw(first, "LOCK");
                    self.set_list_ctx(list);
                }
            }
            Kw::To => {
                if self.saw_rename {
                    self.expect_table = true;
                }
            }
            Kw::Rename => self.saw_rename = true,
            Kw::Using => {
                if self.stmt_type == StmtType::Delete && !self.sig_is(i + 1, b'(') {
                    self.expect_table = true;
                    self.set_list_ctx(true);
                }
            }
            // CREATE INDEX i ON t, CREATE TRIGGER ... ON t.
            Kw::On => self.expect_table = ddl,
            Kw::Like if ddl => self.expect_table = true,
            _ if ddl
                && eq_kw(
                    bytes(self.src, (self.sig[i].start, self.sig[i].end)),
                    "REFERENCES",
                ) =>
            {
                self.expect_table = true;
            }
            _ if info.has(CLAUSE_END) => {
                self.expect_table = false;
                self.set_list_ctx(false);
                if self.cte_depth == Some(self.depth)
                    && matches!(info.kw, Kw::Select | Kw::Values | Kw::Set)
                {
                    self.cte_depth = None;
                }
            }
            _ => {}
        }
    }

    fn set_alias(&mut self, s: Src) {
        if let Some(t) = self.tables.last_mut()
            && t.alias.is_none()
        {
            t.alias = Some(s);
        }
    }

    /// A table reference at `i`: `name` or `db.name`.
    fn table_ref(&mut self, i: usize) -> usize {
        let t = self.sig[i];
        let qualified = self.sig_is(i + 1, b'.')
            && self
                .sig
                .get(i + 2)
                .is_some_and(|n| n.kind.is_ident_like() && n.marker == 0);
        let (db, name, next) = if qualified {
            let n = self.sig[i + 2];
            (Some((t.start, t.end)), (n.start, n.end), i + 3)
        } else {
            (None, (t.start, t.end), i + 1)
        };
        let cte = db.is_none() && !self.cte_names.is_empty() && {
            let v = ident_value(bytes(self.src, name));
            self.cte_names
                .iter()
                .any(|c| ident_value(bytes(self.src, *c)).eq_ignore_ascii_case(&v))
        };
        self.outs.push(Out::Table { db, name, cte });
        self.tables.push(RawTable {
            db,
            name,
            alias: None,
            cte,
        });
        self.expect_table = false;
        self.alias_pending = true;
        next
    }
}

/// Whether a `LIKE` pattern literal starts with a wildcard (`%` or `_`,
/// not escaped).
pub(crate) fn leading_wildcard(tok: &[u8], no_backslash_escapes: bool) -> bool {
    lex::string_value(tok, no_backslash_escapes)
        .is_some_and(|v| matches!(v.first(), Some(b'%' | b'_')))
}

fn contains_password(w: &[u8]) -> bool {
    w.windows(8).any(|x| x.eq_ignore_ascii_case(b"PASSWORD"))
}

/// The type of a `WITH ...` statement: the first `SELECT`, `UPDATE`,
/// `DELETE`, `INSERT` or `REPLACE` at depth 0 after the CTEs.
fn with_stmt_type(src: &[u8], sig: &[Sig]) -> StmtType {
    let mut depth = 0usize;
    for t in sig {
        let b = bytes(src, (t.start, t.end));
        match t.kind {
            TokenKind::Punct if b == b"(" => depth += 1,
            TokenKind::Punct if b == b")" => depth = depth.saturating_sub(1),
            TokenKind::Word if depth == 0 && t.marker == 0 => {
                let st = classify(b).stmt;
                if matches!(
                    st,
                    Some(
                        StmtType::Update | StmtType::Delete | StmtType::Insert | StmtType::Replace
                    )
                ) || st == Some(StmtType::Select) && eq_kw(b, "SELECT")
                {
                    return st.unwrap_or(StmtType::Select);
                }
            }
            _ => {}
        }
    }
    StmtType::Select
}

/// The matching `)` of the `(` at `i`, if any.
fn close_of(outs: &[Out], i: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (k, o) in outs.iter().enumerate().skip(i) {
        match o {
            Out::Punct(b'(') => depth += 1,
            Out::Punct(b')') => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether `o` is a scalar list element: a literal or `NULL`.
fn is_scalar(o: Out) -> bool {
    matches!(o, Out::Lit(_) | Out::WildLit(_) | Out::Kw(_, Kw::Null))
}

/// The list between `outs[a]` = `(` and `outs[b]` = `)`, if every element
/// is a scalar or a parenthesized list of scalars. Returns (elements,
/// tuples among them).
fn literal_list(outs: &[Out], a: usize, b: usize) -> Option<(u32, u32)> {
    let mut k = a + 1;
    let (mut elems, mut tuples) = (0u32, 0u32);
    if k == b {
        return None;
    }
    loop {
        match outs.get(k).copied()? {
            o if is_scalar(o) => k += 1,
            Out::Punct(b'(') => {
                // A tuple of scalars.
                k += 1;
                loop {
                    if !is_scalar(outs.get(k).copied()?) {
                        return None;
                    }
                    k += 1;
                    match outs.get(k).copied()? {
                        Out::Punct(b',') => k += 1,
                        Out::Punct(b')') => break,
                        _ => return None,
                    }
                }
                k += 1;
                tuples += 1;
            }
            _ => return None,
        }
        elems += 1;
        match outs.get(k).copied()? {
            Out::Punct(b',') if k < b => k += 1,
            Out::Punct(b')') if k == b => return Some((elems, tuples)),
            _ => return None,
        }
    }
}

/// The literal range of `outs[a..=b]`.
fn lit_range(outs: &[Out], a: usize, b: usize) -> (u32, u32) {
    let mut first = u32::MAX;
    let mut end = 0u32;
    for o in &outs[a..=b] {
        if let Out::Lit(k) | Out::WildLit(k) = o {
            first = first.min(*k);
            end = end.max(k + 1);
        }
    }
    if first == u32::MAX {
        (0, 0)
    } else {
        (first, end)
    }
}

fn is_row_start(src: &[u8], o: Option<&Out>) -> bool {
    match o {
        Some(Out::Punct(b'(')) => true,
        Some(Out::Func(s)) => eq_kw(bytes(src, *s), "ROW"),
        _ => false,
    }
}

/// Collapses `IN (?, ...)`, `VALUES (...), ...` and lists of literal
/// tuples.
fn collapse(src: &[u8], outs: Vec<Out>) -> Vec<Out> {
    if !outs.contains(&Out::Punct(b'(')) {
        return outs;
    }
    let mut res: Vec<Out> = Vec::with_capacity(outs.len());
    let mut i = 0;
    while i < outs.len() {
        let o = outs[i];
        let prev = res.last().copied();
        // VALUES (...), (...) and VALUES ROW(...), ROW(...); not the
        // VALUES(col) function of ON DUPLICATE KEY UPDATE (after `=`).
        let values = prev.is_some_and(|p| p.is_kw(Kw::Values) || p.is_kw(Kw::Value))
            && !matches!(res.iter().rev().nth(1), Some(Out::Op(_)));
        if values && is_row_start(src, Some(&o)) {
            let mut k = i;
            let mut last = None;
            loop {
                let start = if matches!(outs.get(k), Some(Out::Func(_))) {
                    k + 1
                } else {
                    k
                };
                if outs.get(start) != Some(&Out::Punct(b'(')) {
                    break;
                }
                let Some(c) = close_of(&outs, start) else {
                    break;
                };
                last = Some(c);
                if outs.get(c + 1) == Some(&Out::Punct(b',')) && is_row_start(src, outs.get(c + 2))
                {
                    k = c + 2;
                } else {
                    break;
                }
            }
            if let Some(c) = last {
                let (first, end) = lit_range(&outs, i, c);
                res.push(Out::Collapsed {
                    first,
                    end,
                    in_elems: 0,
                });
                i = c + 1;
                continue;
            }
        }
        if o == Out::Punct(b'(')
            && let Some(c) = close_of(&outs, i)
            && let Some((elems, tuples)) = literal_list(&outs, i, c)
        {
            let after_in = prev.is_some_and(|p| p.is_kw(Kw::In));
            if after_in || tuples >= 2 && tuples == elems {
                let (first, end) = lit_range(&outs, i, c);
                res.push(Out::Collapsed {
                    first,
                    end,
                    in_elems: if after_in { elems } else { 0 },
                });
                i = c + 1;
                continue;
            }
        }
        res.push(o);
        i += 1;
    }
    res
}

/// Whether an identifier's bytes can be written without backquotes: ASCII
/// letters, digits, `_` and `$`, not starting with a digit, and not a
/// keyword (so the text reads back the same).
fn plain_ascii(v: &[u8]) -> bool {
    !v.is_empty()
        && !v[0].is_ascii_digit()
        && v.iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$')
        && !classify(v).has(KEYWORD)
}

/// Writes an identifier unquoted (backquoted only when it must be),
/// lower-cased when `lower`.
fn push_ident(out: &mut Vec<u8>, raw: &[u8], lower: bool) {
    let inner = match raw.first() {
        Some(&q @ (b'`' | b'"')) if raw.len() >= 2 && raw[raw.len() - 1] == q => {
            &raw[1..raw.len() - 1]
        }
        _ => raw,
    };
    if plain_ascii(inner) && !inner.contains(&b'`') {
        if lower {
            out.extend(inner.iter().map(u8::to_ascii_lowercase));
        } else {
            out.extend_from_slice(inner);
        }
        return;
    }
    // Slow path: doubled quotes, other characters, non-ASCII.
    let v = ident_value(raw);
    let v = String::from_utf8_lossy(&v);
    let v = if lower {
        v.to_lowercase()
    } else {
        v.into_owned()
    };
    let quote = v.is_empty()
        || v.as_bytes()[0].is_ascii_digit()
        || classify(v.as_bytes()).has(KEYWORD)
        || v.bytes()
            .any(|b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80));
    if quote {
        out.push(b'`');
        out.extend_from_slice(v.replace('`', "``").as_bytes());
        out.push(b'`');
    } else {
        out.extend_from_slice(v.as_bytes());
    }
}

fn push_upper(out: &mut Vec<u8>, raw: &[u8]) {
    if raw.is_ascii() {
        out.extend(raw.iter().map(u8::to_ascii_uppercase));
    } else {
        out.extend_from_slice(String::from_utf8_lossy(raw).to_uppercase().as_bytes());
    }
}

fn push_lossy(out: &mut Vec<u8>, raw: &[u8]) {
    match std::str::from_utf8(raw) {
        Ok(s) => out.extend_from_slice(s.as_bytes()),
        Err(_) => out.extend_from_slice(String::from_utf8_lossy(raw).as_bytes()),
    }
}

struct Rendered {
    text: String,
    slots: u32,
    in_list_max: u32,
    list_slots: Vec<u32>,
}

/// Renders the output tokens; numbers the slots.
fn render(
    src: &[u8],
    outs: &[Out],
    lits: &mut [Literal],
    current_db: Option<&str>,
    ltn: bool,
) -> Rendered {
    let mut list_slots = Vec::new();
    let mut text: Vec<u8> = Vec::with_capacity(src.len() + 16);
    let mut slot = 0u32;
    let mut in_max = 0u32;
    let mut prev: Option<Out> = None;
    for &o in outs {
        let no_space_before = matches!(o, Out::Punct(b',' | b')' | b'.' | b';'))
            || o == Out::Punct(b'(') && matches!(prev, Some(Out::Func(_)));
        let no_space_after_prev = matches!(prev, Some(Out::Punct(b'(' | b'.')));
        if prev.is_some() && !no_space_before && !no_space_after_prev {
            text.push(b' ');
        }
        match o {
            Out::Kw(s, _) => push_upper(&mut text, bytes(src, s)),
            Out::Func(s) => {
                let b = bytes(src, s);
                if matches!(b.first(), Some(b'`' | b'"')) {
                    let v = ident_value(b);
                    push_ident(
                        &mut text,
                        String::from_utf8_lossy(&v).to_uppercase().as_bytes(),
                        false,
                    );
                } else {
                    push_upper(&mut text, b);
                }
            }
            Out::Ident(s) => push_ident(&mut text, bytes(src, s), true),
            Out::Name(s) => push_ident(&mut text, bytes(src, s), ltn),
            Out::Table { db, name, cte } => {
                if let Some(d) = db {
                    push_ident(&mut text, bytes(src, d), ltn);
                    text.push(b'.');
                } else if let (Some(d), false) = (current_db, cte) {
                    push_ident(&mut text, d.as_bytes(), ltn);
                    text.push(b'.');
                }
                push_ident(&mut text, bytes(src, name), ltn);
            }
            Out::Lit(k) | Out::WildLit(k) => {
                if let Some(l) = lits.get_mut(k as usize) {
                    l.slot = slot;
                }
                slot += 1;
                if matches!(o, Out::WildLit(_)) {
                    text.push(b'%');
                }
                text.push(b'?');
            }
            Out::Collapsed {
                first,
                end,
                in_elems,
            } => {
                for l in lits.iter_mut().take(end as usize).skip(first as usize) {
                    l.slot = slot;
                }
                list_slots.push(slot);
                slot += 1;
                in_max = in_max.max(in_elems);
                text.extend_from_slice(b"(...)");
            }
            Out::Punct(c) => text.push(c),
            Out::Op(s) | Out::Raw(s) => push_lossy(&mut text, bytes(src, s)),
            Out::Var(s) => {
                let b = bytes(src, s);
                if b.is_ascii() {
                    text.extend(b.iter().map(u8::to_ascii_lowercase));
                } else {
                    text.extend_from_slice(String::from_utf8_lossy(b).to_lowercase().as_bytes());
                }
            }
            Out::Static(t) => text.extend_from_slice(t.as_bytes()),
        }
        prev = Some(o);
    }
    let text = String::from_utf8(text)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    Rendered {
        text,
        slots: slot,
        in_list_max: in_max,
        list_slots,
    }
}
