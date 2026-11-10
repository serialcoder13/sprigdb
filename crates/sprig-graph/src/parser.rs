//! The Cypher parser for the subset of chapter 43's table (build step 6):
//! a hand-written lexer and recursive-descent parser into [`crate::ast`].
//! Every error is 39900 (`Neo.ClientError.Statement.SyntaxError`) with the
//! offset and what was expected.
//!
//! As built: `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `RETURN`,
//! `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT`, `UNION [ALL]`,
//! `CREATE`, `MERGE ... ON CREATE / ON MATCH SET`, `SET`, `REMOVE`,
//! `[DETACH] DELETE`, `CALL proc() YIELD`, `EXPLAIN`, `PROFILE`; label
//! expressions; variable-length relationships (`*`, `*n`, `*m..n`) and the
//! quantifier form `{m,n}`; `shortestPath`, `allShortestPaths`, `ANY
//! SHORTEST`, `ALL SHORTEST`; pattern predicates and `EXISTS { }`; `CASE`,
//! list comprehensions, `all/any/none/single`. Anything else is refused
//! here (39900) or by the binder (39901).

use std::collections::BTreeMap;

use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{
    BinOp, Clause, Dir, Entity, Explain, Expr, IndexKind, LabelExpr, MapItem, NodePat, PathKind,
    PathPattern, Projection, Query, RelPat, RemoveItem, Schema, SetItem, SingleQuery, SubqueryKind,
    UnOp,
};
use crate::value::Value;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    /// A backtick-quoted name: never a keyword.
    Quoted(String),
    Int(i64),
    /// 9223372036854775808: valid only after a minus sign.
    MinMagnitude,
    Float(f64),
    Str(String),
    Param(String),
    Sym(&'static str),
    Eof,
}

fn syntax(pos: usize, msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::GraphSyntax,
        format!("Invalid input at offset {pos}: {msg}"),
    )
}

const SYMS: [&str; 30] = [
    "<>", "<=", ">=", "=~", "!=", "..", "+=", "(", ")", "[", "]", "{", "}", ",", ".", ":", ";",
    "|", "+", "-", "*", "/", "%", "^", "=", "<", ">", "&", "!", "$",
];

fn lex(src: &str) -> Result<Vec<(Tok, usize)>> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'/') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let end = src[i + 2..]
                .find("*/")
                .ok_or_else(|| syntax(i, "unterminated comment"))?;
            i += end + 4;
            continue;
        }
        let start = i;
        if c.is_ascii_digit() {
            if c == b'0' && matches!(b.get(i + 1), Some(b'x' | b'X')) {
                i += 2;
                while i < b.len() && b[i].is_ascii_hexdigit() {
                    i += 1;
                }
                let v = i64::from_str_radix(&src[start + 2..i], 16)
                    .map_err(|_| syntax(start, "integer too large"))?;
                out.push((Tok::Int(v), start));
                continue;
            }
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let mut float = false;
            // `1..3` is a range, not a float.
            if i + 1 < b.len() && b[i] == b'.' && b[i + 1].is_ascii_digit() {
                float = true;
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                let mut j = i + 1;
                if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
                    j += 1;
                }
                if j < b.len() && b[j].is_ascii_digit() {
                    float = true;
                    i = j;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let t = &src[start..i];
            out.push((
                if float {
                    Tok::Float(t.parse().map_err(|_| syntax(start, "bad number"))?)
                } else if t == "9223372036854775808" {
                    Tok::MinMagnitude
                } else {
                    Tok::Int(t.parse().map_err(|_| syntax(start, "integer too large"))?)
                },
                start,
            ));
            continue;
        }
        if c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            out.push((
                Tok::Float(
                    src[start..i]
                        .parse()
                        .map_err(|_| syntax(start, "bad number"))?,
                ),
                start,
            ));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] >= 0x80) {
                i += 1;
            }
            out.push((Tok::Ident(src[start..i].to_owned()), start));
            continue;
        }
        if c == b'`' {
            let end = src[i + 1..]
                .find('`')
                .ok_or_else(|| syntax(i, "unterminated quoted name"))?;
            out.push((Tok::Quoted(src[i + 1..i + 1 + end].to_owned()), start));
            i += end + 2;
            continue;
        }
        if c == b'\'' || c == b'"' {
            let q = c;
            i += 1;
            let mut s = String::new();
            loop {
                let Some(&ch) = b.get(i) else {
                    return Err(syntax(start, "unterminated string"));
                };
                if ch == q {
                    i += 1;
                    break;
                }
                if ch == b'\\' {
                    let e = *b.get(i + 1).ok_or_else(|| syntax(i, "bad escape"))?;
                    i += 2;
                    match e {
                        b'n' => s.push('\n'),
                        b't' => s.push('\t'),
                        b'r' => s.push('\r'),
                        b'b' => s.push('\u{8}'),
                        b'f' => s.push('\u{c}'),
                        b'\\' => s.push('\\'),
                        b'\'' => s.push('\''),
                        b'"' => s.push('"'),
                        b'u' => {
                            let h = src
                                .get(i..i + 4)
                                .ok_or_else(|| syntax(i, "bad unicode escape"))?;
                            let cp = u32::from_str_radix(h, 16)
                                .ok()
                                .and_then(char::from_u32)
                                .ok_or_else(|| syntax(i, "bad unicode escape"))?;
                            s.push(cp);
                            i += 4;
                        }
                        _ => return Err(syntax(i - 2, "bad escape")),
                    }
                    continue;
                }
                // Copy one UTF-8 character.
                let len = utf8_len(ch);
                s.push_str(src.get(i..i + len).ok_or_else(|| syntax(i, "bad UTF-8"))?);
                i += len;
            }
            out.push((Tok::Str(s), start));
            continue;
        }
        if c == b'$' {
            i += 1;
            let ps = i;
            if b.get(i) == Some(&b'`') {
                let end = src[i + 1..]
                    .find('`')
                    .ok_or_else(|| syntax(i, "unterminated quoted name"))?;
                out.push((Tok::Param(src[i + 1..i + 1 + end].to_owned()), start));
                i += end + 2;
                continue;
            }
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            if ps == i {
                return Err(syntax(start, "a parameter needs a name"));
            }
            out.push((Tok::Param(src[ps..i].to_owned()), start));
            continue;
        }
        let Some(sym) = SYMS.iter().find(|s| src[i..].starts_with(**s)) else {
            return Err(syntax(i, format!("unexpected character '{}'", c as char)));
        };
        i += sym.len();
        out.push((Tok::Sym(sym), start));
    }
    out.push((Tok::Eof, src.len()));
    Ok(out)
}

fn utf8_len(b: u8) -> usize {
    match b {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

/// Parses one statement.
///
/// # Errors
/// 39900 with the offset.
pub fn parse(src: &str) -> Result<Query> {
    // GQL's forms become Cypher first (round 3, `crate::gql`).
    let rewritten = crate::gql::rewrite(src)?;
    let src = rewritten.as_str();
    let toks = lex(src)?;
    let mut p = Parser {
        src: src.to_owned(),
        toks,
        pos: 0,
    };
    let q = p.query()?;
    p.accept_sym(";");
    if !matches!(p.peek(), Tok::Eof) {
        return Err(p.err("end of statement"));
    }
    crate::semantic::check(&q)?;
    Ok(q)
}

/// Parses a standalone expression (tests, `GRAPH_TABLE` columns).
///
/// # Errors
/// 39900.
pub fn parse_expr(src: &str) -> Result<Expr> {
    let toks = lex(src)?;
    let mut p = Parser {
        src: src.to_owned(),
        toks,
        pos: 0,
    };
    let e = p.expr()?;
    if !matches!(p.peek(), Tok::Eof) {
        return Err(p.err("end of expression"));
    }
    Ok(e)
}

struct Parser {
    src: String,
    toks: Vec<(Tok, usize)>,
    pos: usize,
}

const RESERVED: [&str; 35] = [
    "MATCH", "OPTIONAL", "WHERE", "RETURN", "WITH", "UNWIND", "CREATE", "MERGE", "SET", "DELETE",
    "DETACH", "REMOVE", "ORDER", "SKIP", "LIMIT", "UNION", "CALL", "YIELD", "AND", "OR", "XOR",
    "NOT", "IN", "AS", "ON", "CASE", "WHEN", "THEN", "ELSE", "END", "IS", "STARTS", "ENDS",
    "CONTAINS", "FOREACH",
];

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos].0
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.pos + n).min(self.toks.len() - 1)].0
    }

    fn offset(&self) -> usize {
        self.toks[self.pos].1
    }

    fn err(&self, expected: &str) -> SprigError {
        let got = match self.peek() {
            Tok::Ident(s) | Tok::Quoted(s) | Tok::Str(s) => format!("'{s}'"),
            Tok::Int(i) => i.to_string(),
            Tok::MinMagnitude => "9223372036854775808".into(),
            Tok::Float(f) => f.to_string(),
            Tok::Param(p) => format!("${p}"),
            Tok::Sym(s) => format!("'{s}'"),
            Tok::Eof => "end of input".into(),
        };
        syntax(self.offset(), format!("expected {expected}, found {got}"))
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        matches!(self.peek_at(n), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn accept_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.accept_kw(kw) {
            Ok(())
        } else {
            Err(self.err(kw))
        }
    }

    fn is_sym(&self, s: &str) -> bool {
        matches!(self.peek(), Tok::Sym(x) if *x == s)
    }

    fn is_sym_at(&self, n: usize, s: &str) -> bool {
        matches!(self.peek_at(n), Tok::Sym(x) if *x == s)
    }

    fn accept_sym(&mut self, s: &str) -> bool {
        if self.is_sym(s) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_sym(&mut self, s: &str) -> Result<()> {
        if self.accept_sym(s) {
            Ok(())
        } else {
            Err(self.err(&format!("'{s}'")))
        }
    }

    /// A name: an identifier or a quoted name.
    fn name(&mut self) -> Result<String> {
        match self.peek().clone() {
            Tok::Ident(s) | Tok::Quoted(s) => {
                self.bump();
                Ok(s)
            }
            _ => Err(self.err("a name")),
        }
    }

    /// A variable: a name that isn't a reserved word.
    fn var(&mut self) -> Result<String> {
        match self.peek().clone() {
            Tok::Ident(s) if !RESERVED.iter().any(|r| r.eq_ignore_ascii_case(&s)) => {
                self.bump();
                Ok(s)
            }
            Tok::Quoted(s) => {
                self.bump();
                Ok(s)
            }
            _ => Err(self.err("a variable")),
        }
    }

    fn is_var_start(&self) -> bool {
        match self.peek() {
            Tok::Ident(s) => !RESERVED.iter().any(|r| r.eq_ignore_ascii_case(s)),
            Tok::Quoted(_) => true,
            _ => false,
        }
    }

    // ---------------------------------------------------------------
    // Statements and clauses
    // ---------------------------------------------------------------

    fn query(&mut self) -> Result<Query> {
        let explain = if self.accept_kw("EXPLAIN") {
            Explain::Explain
        } else if self.accept_kw("PROFILE") {
            Explain::Profile
        } else {
            Explain::None
        };
        if self.is_kw("USE") {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "USE is not supported: name the graph as the Bolt database",
            ));
        }
        if let Some(s) = self.schema()? {
            return Ok(Query {
                explain,
                parts: vec![SingleQuery {
                    clauses: vec![Clause::Schema(s)],
                }],
                union_all: vec![],
            });
        }
        let mut q = self.query_inner()?;
        q.explain = explain;
        Ok(q)
    }

    /// Single queries joined by `UNION`, up to the end or a `}`.
    fn query_inner(&mut self) -> Result<Query> {
        let mut parts = vec![self.single()?];
        let mut union_all = Vec::new();
        while self.accept_kw("UNION") {
            union_all.push(self.accept_kw("ALL"));
            parts.push(self.single()?);
        }
        Ok(Query {
            explain: Explain::None,
            parts,
            union_all,
        })
    }

    /// `{ query }` after `CALL`, `EXISTS`, `COUNT` or `COLLECT`.
    fn braced_query(&mut self) -> Result<Query> {
        self.expect_sym("{")?;
        let q = self.query_inner()?;
        self.expect_sym("}")?;
        Ok(q)
    }

    fn is_clause_start(&self) -> bool {
        [
            "MATCH", "OPTIONAL", "UNWIND", "WITH", "CALL", "RETURN", "CREATE", "MERGE",
        ]
        .iter()
        .any(|k| self.is_kw(k))
    }

    /// An index or constraint command, or `None`.
    fn schema(&mut self) -> Result<Option<Schema>> {
        let unsupported = |what: &str| {
            SprigError::new(
                SprigCode::GraphUnsupported,
                format!("{what} is not supported yet"),
            )
        };
        if self.is_kw("DROP") && (self.is_kw_at(1, "INDEX") || self.is_kw_at(1, "CONSTRAINT")) {
            self.bump();
            let constraint = self.accept_kw("CONSTRAINT");
            if !constraint {
                self.expect_kw("INDEX")?;
            }
            let name = self.name()?;
            let if_exists = if self.accept_kw("IF") {
                self.expect_kw("EXISTS")?;
                true
            } else {
                false
            };
            return Ok(Some(Schema::Drop {
                constraint,
                name,
                if_exists,
            }));
        }
        if !self.is_kw("CREATE") {
            return Ok(None);
        }
        let mut n = 1;
        let mut kind = IndexKind::Range;
        if self.is_kw_at(1, "RANGE") || self.is_kw_at(1, "TEXT") {
            if self.is_kw_at(1, "TEXT") {
                kind = IndexKind::Text;
            }
            n = 2;
        } else if ["POINT", "FULLTEXT", "VECTOR", "LOOKUP", "BTREE"]
            .iter()
            .any(|k| self.is_kw_at(1, k))
            && self.is_kw_at(2, "INDEX")
        {
            let what = match self.peek_at(1) {
                Tok::Ident(s) => s.to_uppercase(),
                _ => String::new(),
            };
            return Err(unsupported(&format!("CREATE {what} INDEX")));
        }
        let unique = self.is_kw_at(n, "CONSTRAINT");
        if !unique && !self.is_kw_at(n, "INDEX") {
            return Ok(None);
        }
        for _ in 0..=n {
            self.bump();
        }
        let name = if self.is_kw("FOR") || (self.is_kw("IF") && self.is_kw_at(1, "NOT")) {
            None
        } else {
            Some(self.name()?)
        };
        let if_not_exists = if self.accept_kw("IF") {
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        self.expect_kw("FOR")?;
        // (n:L) or ()-[r:T]-()
        self.expect_sym("(")?;
        let (entity, var, token) = if self.accept_sym(")") {
            let _ = self.accept_sym("<");
            self.expect_sym("-")?;
            self.expect_sym("[")?;
            let v = self.var()?;
            self.expect_sym(":")?;
            let t = self.name()?;
            self.expect_sym("]")?;
            self.expect_sym("-")?;
            let _ = self.accept_sym(">");
            self.expect_sym("(")?;
            self.expect_sym(")")?;
            (Entity::Relationship, v, t)
        } else {
            let v = self.var()?;
            self.expect_sym(":")?;
            let l = self.name()?;
            self.expect_sym(")")?;
            (Entity::Node, v, l)
        };
        let mut props = Vec::new();
        let mut prop = |p: &mut Self| -> Result<()> {
            let v = p.var()?;
            if v != var {
                return Err(p.err(&format!("a property of `{var}`")));
            }
            p.expect_sym(".")?;
            props.push(p.name()?);
            Ok(())
        };
        if unique {
            self.expect_kw("REQUIRE")?;
            if self.accept_sym("(") {
                loop {
                    prop(self)?;
                    if !self.accept_sym(",") {
                        break;
                    }
                }
                self.expect_sym(")")?;
            } else {
                prop(self)?;
            }
            self.expect_kw("IS")?;
            if !self.accept_kw("UNIQUE") {
                return Err(unsupported(
                    "a constraint other than IS UNIQUE (existence, key and type constraints)",
                ));
            }
        } else {
            self.expect_kw("ON")?;
            self.expect_sym("(")?;
            loop {
                prop(self)?;
                if !self.accept_sym(",") {
                    break;
                }
            }
            self.expect_sym(")")?;
        }
        if self.accept_kw("OPTIONS") {
            // Index providers and configuration don't apply here.
            let _ = self.map_literal()?;
        }
        if kind == IndexKind::Text && props.len() != 1 {
            return Err(self.err("one property for a TEXT index"));
        }
        Ok(Some(Schema::Create {
            name,
            if_not_exists,
            unique,
            kind,
            entity,
            token,
            props,
        }))
    }

    /// `SHOW ... INDEXES|CONSTRAINTS [YIELD ...] [WHERE ...]` as a call of
    /// `sprig.show.indexes` or `sprig.show.constraints`, the filter word
    /// as its argument.
    fn show(&mut self) -> Result<Clause> {
        self.expect_kw("SHOW")?;
        let filter = match self.peek() {
            Tok::Ident(s)
                if !["INDEX", "INDEXES", "CONSTRAINT", "CONSTRAINTS"]
                    .iter()
                    .any(|k| s.eq_ignore_ascii_case(k)) =>
            {
                let f = s.to_ascii_uppercase();
                self.bump();
                f
            }
            _ => "ALL".to_owned(),
        };
        let name = if self.accept_kw("INDEX") || self.accept_kw("INDEXES") {
            "sprig.show.indexes"
        } else if self.accept_kw("CONSTRAINT") || self.accept_kw("CONSTRAINTS") {
            "sprig.show.constraints"
        } else {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "SHOW supports INDEXES and CONSTRAINTS",
            ));
        };
        let mut yields = None;
        // ORDER BY, SKIP and LIMIT after YIELD are left to RETURN.
        if self.accept_kw("YIELD") && !self.accept_sym("*") {
            let mut y = Vec::new();
            loop {
                let col = self.name()?;
                let alias = if self.accept_kw("AS") {
                    self.var()?
                } else {
                    col.clone()
                };
                y.push((col, alias));
                if !self.accept_sym(",") {
                    break;
                }
            }
            yields = Some(y);
        }
        let where_ = if self.accept_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Clause::Call {
            name: name.to_owned(),
            args: Some(vec![Expr::Lit(Value::String(filter))]),
            yields,
            where_,
        })
    }

    fn single(&mut self) -> Result<SingleQuery> {
        let mut clauses = Vec::new();
        loop {
            let c = if self.is_kw("MATCH") || (self.is_kw("OPTIONAL") && self.is_kw_at(1, "MATCH"))
            {
                let optional = self.accept_kw("OPTIONAL");
                self.expect_kw("MATCH")?;
                let patterns = self.patterns()?;
                let where_ = if self.accept_kw("WHERE") {
                    Some(self.expr()?)
                } else {
                    None
                };
                Clause::Match {
                    optional,
                    patterns,
                    where_,
                }
            } else if self.accept_kw("UNWIND") {
                let expr = self.expr()?;
                self.expect_kw("AS")?;
                Clause::Unwind {
                    expr,
                    alias: self.var()?,
                }
            } else if self.accept_kw("WITH") {
                Clause::With(self.projection(true)?)
            } else if self.accept_kw("RETURN") {
                let p = self.projection(false)?;
                clauses.push(Clause::Return(p));
                break;
            } else if self.accept_kw("CREATE") {
                if self.is_kw("INDEX") || self.is_kw("CONSTRAINT") || self.is_kw("OR") {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "index and constraint commands stand alone",
                    ));
                }
                Clause::Create(self.patterns()?)
            } else if self.accept_kw("MERGE") {
                let pattern = self.path_pattern()?;
                let (mut on_create, mut on_match) = (Vec::new(), Vec::new());
                while self.is_kw("ON") {
                    self.bump();
                    if self.accept_kw("CREATE") {
                        self.expect_kw("SET")?;
                        on_create.extend(self.set_items()?);
                    } else {
                        self.expect_kw("MATCH")?;
                        self.expect_kw("SET")?;
                        on_match.extend(self.set_items()?);
                    }
                }
                Clause::Merge {
                    pattern,
                    on_create,
                    on_match,
                }
            } else if self.accept_kw("SET") {
                Clause::Set(self.set_items()?)
            } else if self.accept_kw("REMOVE") {
                Clause::Remove(self.remove_items()?)
            } else if self.is_kw("DELETE") || self.is_kw("DETACH") {
                let detach = self.accept_kw("DETACH");
                self.expect_kw("DELETE")?;
                let mut exprs = vec![self.expr()?];
                while self.accept_sym(",") {
                    exprs.push(self.expr()?);
                }
                Clause::Delete { detach, exprs }
            } else if self.accept_kw("CALL") {
                if self.is_sym("{") || self.is_sym("(") {
                    let imports = if self.accept_sym("(") {
                        let mut v = Vec::new();
                        if self.accept_sym("*") {
                            v.push("*".to_owned());
                        } else if !self.is_sym(")") {
                            v.push(self.var()?);
                            while self.accept_sym(",") {
                                v.push(self.var()?);
                            }
                        }
                        self.expect_sym(")")?;
                        Some(v)
                    } else {
                        None
                    };
                    let query = Box::new(self.braced_query()?);
                    if self.is_kw("IN") && self.is_kw_at(1, "TRANSACTIONS") {
                        return Err(SprigError::new(
                            SprigCode::GraphUnsupported,
                            "CALL { } IN TRANSACTIONS is not supported yet",
                        ));
                    }
                    Clause::Subquery { imports, query }
                } else {
                    self.call()?
                }
            } else if self.accept_kw("FOREACH") {
                self.expect_sym("(")?;
                let var = self.var()?;
                self.expect_kw("IN")?;
                let list = self.expr()?;
                self.expect_sym("|")?;
                let body = self.single()?.clauses;
                for c in &body {
                    if !matches!(
                        c,
                        Clause::Create(_)
                            | Clause::Merge { .. }
                            | Clause::Set(_)
                            | Clause::Remove(_)
                            | Clause::Delete { .. }
                            | Clause::Foreach { .. }
                    ) {
                        return Err(self.err("only updates (CREATE, MERGE, SET, REMOVE, DELETE, FOREACH) inside FOREACH"));
                    }
                }
                self.expect_sym(")")?;
                Clause::Foreach { var, list, body }
            } else if self.is_kw("SHOW") {
                self.show()?
            } else if self.is_kw("LOAD") {
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    format!("{} is not supported", self.name()?.to_uppercase()),
                ));
            } else {
                if clauses.is_empty() {
                    return Err(self.err("a clause (MATCH, CREATE, RETURN, ...)"));
                }
                break;
            };
            clauses.push(c);
        }
        Ok(SingleQuery { clauses })
    }

    fn call(&mut self) -> Result<Clause> {
        let mut name = self.name()?;
        while self.accept_sym(".") {
            name.push('.');
            name.push_str(&self.name()?);
        }
        let args = if self.accept_sym("(") {
            let mut a = Vec::new();
            if !self.is_sym(")") {
                a.push(self.expr()?);
                while self.accept_sym(",") {
                    a.push(self.expr()?);
                }
            }
            self.expect_sym(")")?;
            Some(a)
        } else {
            None
        };
        let mut yields = None;
        let mut where_ = None;
        if self.accept_kw("YIELD") {
            let mut y = Vec::new();
            if self.accept_sym("*") {
                yields = None;
            } else {
                loop {
                    let col = self.name()?;
                    let alias = if self.accept_kw("AS") {
                        self.var()?
                    } else {
                        col.clone()
                    };
                    y.push((col, alias));
                    if !self.accept_sym(",") {
                        break;
                    }
                }
                yields = Some(y);
            }
            if self.accept_kw("WHERE") {
                where_ = Some(self.expr()?);
            }
        }
        Ok(Clause::Call {
            name: name.to_ascii_lowercase(),
            args,
            yields,
            where_,
        })
    }

    fn projection(&mut self, with: bool) -> Result<Projection> {
        let distinct = self.accept_kw("DISTINCT");
        let mut star = false;
        let mut items = Vec::new();
        if self.accept_sym("*") {
            star = true;
            if !self.accept_sym(",") {
                return self.projection_tail(with, distinct, star, items);
            }
        }
        loop {
            let start = self.offset();
            let e = self.expr()?;
            let end = self.offset();
            let alias = if self.accept_kw("AS") {
                self.var()?
            } else if let Expr::Var(v) = &e {
                v.clone()
            } else {
                if with {
                    return Err(syntax(
                        start,
                        "expressions in WITH must be aliased (use AS)",
                    ));
                }
                self.text(start, end)
            };
            items.push((e, alias));
            if !self.accept_sym(",") {
                break;
            }
        }
        self.projection_tail(with, distinct, star, items)
    }

    fn text(&self, start: usize, end: usize) -> String {
        self.src
            .get(start..end)
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn projection_tail(
        &mut self,
        with: bool,
        distinct: bool,
        star: bool,
        items: Vec<(Expr, String)>,
    ) -> Result<Projection> {
        let mut order = Vec::new();
        if self.accept_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let e = self.expr()?;
                let asc = if self.accept_kw("DESC") || self.accept_kw("DESCENDING") {
                    false
                } else {
                    let _ = self.accept_kw("ASC") || self.accept_kw("ASCENDING");
                    true
                };
                order.push((e, asc));
                if !self.accept_sym(",") {
                    break;
                }
            }
        }
        let skip = if self.accept_kw("SKIP") || self.accept_kw("OFFSET") {
            Some(self.expr()?)
        } else {
            None
        };
        let limit = if self.accept_kw("LIMIT") {
            Some(self.expr()?)
        } else {
            None
        };
        let where_ = if with && self.accept_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Projection {
            distinct,
            star,
            items,
            order,
            skip,
            limit,
            where_,
        })
    }

    fn set_items(&mut self) -> Result<Vec<SetItem>> {
        let mut out = Vec::new();
        loop {
            let v = self.var()?;
            if self.is_sym(":") {
                out.push(SetItem::Labels(v, self.label_list()?));
            } else if self.accept_sym("=") {
                out.push(SetItem::Replace(v, self.expr()?));
            } else if self.accept_sym("+=") {
                out.push(SetItem::Merge(v, self.expr()?));
            } else {
                let mut target = Expr::Var(v);
                let mut key = None;
                while self.accept_sym(".") {
                    if let Some(k) = key.take() {
                        target = Expr::Prop(Box::new(target), k);
                    }
                    key = Some(self.name()?);
                }
                let Some(key) = key else {
                    return Err(self.err("'.', ':', '=' or '+='"));
                };
                self.expect_sym("=")?;
                out.push(SetItem::Prop(target, key, self.expr()?));
            }
            if !self.accept_sym(",") {
                return Ok(out);
            }
        }
    }

    fn remove_items(&mut self) -> Result<Vec<RemoveItem>> {
        let mut out = Vec::new();
        loop {
            let v = self.var()?;
            if self.is_sym(":") {
                out.push(RemoveItem::Labels(v, self.label_list()?));
            } else {
                self.expect_sym(".")?;
                out.push(RemoveItem::Prop(Expr::Var(v), self.name()?));
            }
            if !self.accept_sym(",") {
                return Ok(out);
            }
        }
    }

    fn label_list(&mut self) -> Result<Vec<String>> {
        let mut l = Vec::new();
        while self.accept_sym(":") {
            l.push(self.name()?);
        }
        Ok(l)
    }

    // ---------------------------------------------------------------
    // Patterns
    // ---------------------------------------------------------------

    fn patterns(&mut self) -> Result<Vec<PathPattern>> {
        let mut v = vec![self.path_pattern()?];
        while self.accept_sym(",") {
            v.push(self.path_pattern()?);
        }
        Ok(v)
    }

    fn path_pattern(&mut self) -> Result<PathPattern> {
        let var = if self.is_var_start() && self.is_sym_at(1, "=") {
            let v = self.var()?;
            self.bump();
            Some(v)
        } else {
            None
        };
        let kind = if self.is_kw("shortestPath") && self.is_sym_at(1, "(") {
            PathKind::Shortest
        } else if self.is_kw("allShortestPaths") && self.is_sym_at(1, "(") {
            PathKind::AllShortest
        } else if self.is_kw("ANY") && self.is_kw_at(1, "SHORTEST") {
            self.bump();
            self.bump();
            let mut p = self.pattern_body()?;
            p.var = var;
            p.kind = PathKind::Shortest;
            return Ok(p);
        } else if self.is_kw("ALL") && self.is_kw_at(1, "SHORTEST") {
            self.bump();
            self.bump();
            let mut p = self.pattern_body()?;
            p.var = var;
            p.kind = PathKind::AllShortest;
            return Ok(p);
        } else if self.is_kw("SHORTEST") {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "SHORTEST k is not supported yet",
            ));
        } else {
            PathKind::Normal
        };
        let mut p = if kind == PathKind::Normal {
            self.pattern_body()?
        } else {
            self.bump();
            self.expect_sym("(")?;
            let p = self.pattern_body()?;
            self.expect_sym(")")?;
            p
        };
        p.var = var;
        p.kind = kind;
        Ok(p)
    }

    fn pattern_body(&mut self) -> Result<PathPattern> {
        let start = self.node_pat()?;
        let mut steps = Vec::new();
        while self.is_sym("-") || (self.is_sym("<") && self.is_sym_at(1, "-")) {
            let rel = self.rel_pat()?;
            let node = self.node_pat()?;
            steps.push((rel, node));
        }
        Ok(PathPattern {
            var: None,
            kind: PathKind::Normal,
            start,
            steps,
        })
    }

    fn node_pat(&mut self) -> Result<NodePat> {
        self.expect_sym("(")?;
        let var = if self.is_var_start() {
            Some(self.var()?)
        } else {
            None
        };
        let labels = if self.accept_sym(":") {
            Some(self.label_expr(true)?)
        } else if self.is_kw("IS") {
            self.bump();
            Some(self.label_expr(false)?)
        } else {
            None
        };
        let props = if self.is_sym("{") {
            Some(self.map_literal()?)
        } else if self.is_sym("$") {
            return Err(self.err("a property map"));
        } else if let Tok::Param(p) = self.peek().clone() {
            self.bump();
            Some(Expr::Param(p))
        } else {
            None
        };
        if self.is_kw("WHERE") {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "WHERE inside a node pattern is not supported yet",
            ));
        }
        self.expect_sym(")")?;
        Ok(NodePat { var, labels, props })
    }

    /// `A:B`, `A&B`, `A|B`, `!A`, `%`, `(A|B)&C`; `colon_chain` allows
    /// `:A:B`.
    fn label_expr(&mut self, colon_chain: bool) -> Result<LabelExpr> {
        let mut alts = vec![self.label_and(colon_chain)?];
        while self.accept_sym("|") {
            let _ = self.accept_sym(":");
            alts.push(self.label_and(colon_chain)?);
        }
        Ok(if alts.len() == 1 {
            alts.remove(0)
        } else {
            LabelExpr::Or(alts)
        })
    }

    fn label_and(&mut self, colon_chain: bool) -> Result<LabelExpr> {
        let mut all = vec![self.label_atom(colon_chain)?];
        loop {
            if self.accept_sym("&") || (colon_chain && self.accept_sym(":")) {
                all.push(self.label_atom(colon_chain)?);
            } else {
                break;
            }
        }
        Ok(if all.len() == 1 {
            all.remove(0)
        } else {
            LabelExpr::And(all)
        })
    }

    fn label_atom(&mut self, colon_chain: bool) -> Result<LabelExpr> {
        if self.accept_sym("!") {
            return Ok(LabelExpr::Not(Box::new(self.label_atom(colon_chain)?)));
        }
        if self.accept_sym("%") {
            return Ok(LabelExpr::Any);
        }
        if self.accept_sym("(") {
            let e = self.label_expr(colon_chain)?;
            self.expect_sym(")")?;
            return Ok(e);
        }
        Ok(LabelExpr::Name(self.name()?))
    }

    fn rel_pat(&mut self) -> Result<RelPat> {
        let left = self.accept_sym("<");
        self.expect_sym("-")?;
        let mut var = None;
        let mut types = Vec::new();
        let mut props = None;
        let mut range = None;
        if self.accept_sym("[") {
            if self.is_var_start() {
                var = Some(self.var()?);
            }
            if self.accept_sym(":")
                || (self.is_kw("IS") && {
                    self.bump();
                    true
                })
            {
                types.push(self.name()?);
                while self.accept_sym("|") {
                    let _ = self.accept_sym(":");
                    types.push(self.name()?);
                }
            }
            if self.accept_sym("*") {
                let lo = self.opt_u32()?;
                if self.accept_sym("..") {
                    range = Some((lo, self.opt_u32()?));
                } else {
                    range = Some(match lo {
                        Some(n) => (Some(n), Some(n)),
                        None => (None, None),
                    });
                }
            }
            if self.is_sym("{") {
                props = Some(self.map_literal()?);
            } else if let Tok::Param(p) = self.peek().clone() {
                self.bump();
                props = Some(Expr::Param(p));
            }
            if self.is_kw("WHERE") {
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    "WHERE inside a relationship pattern is not supported yet",
                ));
            }
            self.expect_sym("]")?;
        }
        self.expect_sym("-")?;
        let right = self.accept_sym(">");
        let dir = match (left, right) {
            (true, false) => Dir::In,
            (false, true) => Dir::Out,
            (false, false) => Dir::Both,
            (true, true) => return Err(self.err("one direction")),
        };
        // GQL / Neo4j 5 quantifier after the relationship: `{m,n}`, `+`, `*`.
        if range.is_none() {
            if self.is_sym("{") && matches!(self.peek_at(1), Tok::Int(_) | Tok::Sym(",")) {
                self.bump();
                let lo = self.opt_u32()?;
                let hi = if self.accept_sym(",") {
                    self.opt_u32()?
                } else {
                    lo
                };
                self.expect_sym("}")?;
                range = Some((lo.or(Some(0)), hi));
            } else if self.is_sym("+") && self.is_sym_at(1, "(") {
                self.bump();
                range = Some((Some(1), None));
            } else if self.is_sym("*") && self.is_sym_at(1, "(") {
                self.bump();
                range = Some((Some(0), None));
            }
        }
        Ok(RelPat {
            var,
            types,
            dir,
            props,
            range,
        })
    }

    fn opt_u32(&mut self) -> Result<Option<u32>> {
        if let Tok::Int(n) = self.peek().clone() {
            self.bump();
            return u32::try_from(n)
                .map(Some)
                .map_err(|_| syntax(self.offset(), "a length must be 0 or more"));
        }
        Ok(None)
    }

    fn map_literal(&mut self) -> Result<Expr> {
        self.expect_sym("{")?;
        let mut m = Vec::new();
        if !self.is_sym("}") {
            loop {
                let k = self.name()?;
                self.expect_sym(":")?;
                m.push((k, self.expr()?));
                if !self.accept_sym(",") {
                    break;
                }
            }
        }
        self.expect_sym("}")?;
        Ok(Expr::Map(m))
    }

    // ---------------------------------------------------------------
    // Expressions
    // ---------------------------------------------------------------

    fn expr(&mut self) -> Result<Expr> {
        self.or()
    }

    fn or(&mut self) -> Result<Expr> {
        let mut l = self.xor()?;
        while self.accept_kw("OR") {
            l = Expr::Binary(BinOp::Or, Box::new(l), Box::new(self.xor()?));
        }
        Ok(l)
    }

    fn xor(&mut self) -> Result<Expr> {
        let mut l = self.and()?;
        while self.accept_kw("XOR") {
            l = Expr::Binary(BinOp::Xor, Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }

    fn and(&mut self) -> Result<Expr> {
        let mut l = self.not()?;
        while self.accept_kw("AND") {
            l = Expr::Binary(BinOp::And, Box::new(l), Box::new(self.not()?));
        }
        Ok(l)
    }

    fn not(&mut self) -> Result<Expr> {
        if self.accept_kw("NOT") {
            return Ok(Expr::Unary(UnOp::Not, Box::new(self.not()?)));
        }
        self.comparison()
    }

    /// Comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`), chained as openCypher
    /// says: `a < b <= c` is `a < b AND b <= c`. Their operands are the
    /// string, list and null predicates, which bind tighter.
    fn comparison(&mut self) -> Result<Expr> {
        let first = self.predicate()?;
        let mut operands = vec![first];
        let mut ops = Vec::new();
        loop {
            let op = match self.peek() {
                Tok::Sym("=") => BinOp::Eq,
                Tok::Sym("<>" | "!=") => BinOp::Ne,
                Tok::Sym("<") => BinOp::Lt,
                Tok::Sym("<=") => BinOp::Le,
                Tok::Sym(">") => BinOp::Gt,
                Tok::Sym(">=") => BinOp::Ge,
                _ => break,
            };
            self.bump();
            ops.push(op);
            operands.push(self.predicate()?);
        }
        if ops.is_empty() {
            return Ok(operands.pop().unwrap_or(Expr::Lit(Value::Null)));
        }
        let mut out: Option<Expr> = None;
        for (i, op) in ops.into_iter().enumerate() {
            let c = Expr::Binary(
                op,
                Box::new(operands[i].clone()),
                Box::new(operands[i + 1].clone()),
            );
            out = Some(match out {
                None => c,
                Some(prev) => Expr::Binary(BinOp::And, Box::new(prev), Box::new(c)),
            });
        }
        Ok(out.unwrap_or(Expr::Lit(Value::Null)))
    }

    /// `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~` and `IS [NOT]
    /// NULL` over additive expressions.
    fn predicate(&mut self) -> Result<Expr> {
        let mut l = self.additive()?;
        loop {
            let op = match self.peek() {
                Tok::Sym("=~") => BinOp::Regex,
                Tok::Ident(s) if s.eq_ignore_ascii_case("IN") => BinOp::In,
                Tok::Ident(s) if s.eq_ignore_ascii_case("CONTAINS") => BinOp::Contains,
                Tok::Ident(s) if s.eq_ignore_ascii_case("STARTS") => {
                    self.bump();
                    self.expect_kw("WITH")?;
                    l = Expr::Binary(BinOp::StartsWith, Box::new(l), Box::new(self.additive()?));
                    continue;
                }
                Tok::Ident(s) if s.eq_ignore_ascii_case("ENDS") => {
                    self.bump();
                    self.expect_kw("WITH")?;
                    l = Expr::Binary(BinOp::EndsWith, Box::new(l), Box::new(self.additive()?));
                    continue;
                }
                Tok::Ident(s) if s.eq_ignore_ascii_case("IS") => {
                    self.bump();
                    let neg = self.accept_kw("NOT");
                    self.expect_kw("NULL")?;
                    l = Expr::IsNull(Box::new(l), neg);
                    continue;
                }
                _ => return Ok(l),
            };
            self.bump();
            let r = self.additive()?;
            l = Expr::Binary(op, Box::new(l), Box::new(r));
        }
    }

    fn additive(&mut self) -> Result<Expr> {
        let mut l = self.multiplicative()?;
        loop {
            let op = if self.is_sym("+") {
                BinOp::Add
            } else if self.is_sym("-") {
                BinOp::Sub
            } else {
                return Ok(l);
            };
            self.bump();
            l = Expr::Binary(op, Box::new(l), Box::new(self.multiplicative()?));
        }
    }

    fn multiplicative(&mut self) -> Result<Expr> {
        let mut l = self.power()?;
        loop {
            let op = if self.is_sym("*") {
                BinOp::Mul
            } else if self.is_sym("/") {
                BinOp::Div
            } else if self.is_sym("%") {
                BinOp::Mod
            } else {
                return Ok(l);
            };
            self.bump();
            l = Expr::Binary(op, Box::new(l), Box::new(self.power()?));
        }
    }

    fn power(&mut self) -> Result<Expr> {
        let l = self.unary()?;
        if self.accept_sym("^") {
            return Ok(Expr::Binary(
                BinOp::Pow,
                Box::new(l),
                Box::new(self.power()?),
            ));
        }
        Ok(l)
    }

    fn unary(&mut self) -> Result<Expr> {
        if self.accept_sym("-") {
            // Fold negative literals so i64::MIN parses.
            if let Tok::Int(i) = self.peek().clone() {
                self.bump();
                return self.postfix(Expr::Lit(Value::Int(i.wrapping_neg())));
            }
            if matches!(self.peek(), Tok::MinMagnitude) {
                self.bump();
                return self.postfix(Expr::Lit(Value::Int(i64::MIN)));
            }
            return Ok(Expr::Unary(UnOp::Neg, Box::new(self.unary()?)));
        }
        if self.accept_sym("+") {
            return Ok(Expr::Unary(UnOp::Plus, Box::new(self.unary()?)));
        }
        let a = self.atom()?;
        self.postfix(a)
    }

    fn postfix(&mut self, mut e: Expr) -> Result<Expr> {
        loop {
            if self.is_sym(".") {
                self.bump();
                e = Expr::Prop(Box::new(e), self.name()?);
            } else if self.is_sym("[") {
                self.bump();
                if self.accept_sym("..") {
                    let hi = if self.is_sym("]") {
                        None
                    } else {
                        Some(Box::new(self.expr()?))
                    };
                    self.expect_sym("]")?;
                    e = Expr::Slice(Box::new(e), None, hi);
                    continue;
                }
                let idx = self.expr()?;
                if self.accept_sym("..") {
                    let hi = if self.is_sym("]") {
                        None
                    } else {
                        Some(Box::new(self.expr()?))
                    };
                    self.expect_sym("]")?;
                    e = Expr::Slice(Box::new(e), Some(Box::new(idx)), hi);
                } else {
                    self.expect_sym("]")?;
                    e = Expr::Index(Box::new(e), Box::new(idx));
                }
            } else if self.is_sym(":") && matches!(e, Expr::Var(_)) {
                self.bump();
                e = Expr::HasLabels(Box::new(e), self.label_expr(true)?);
            } else if self.is_sym("{") && matches!(e, Expr::Var(_)) {
                e = self.map_projection(e)?;
            } else {
                return Ok(e);
            }
        }
    }

    /// `{.a, .*, k: e, v}` after a variable.
    fn map_projection(&mut self, base: Expr) -> Result<Expr> {
        self.expect_sym("{")?;
        let mut items = Vec::new();
        if !self.is_sym("}") {
            loop {
                if self.accept_sym(".") {
                    if self.accept_sym("*") {
                        items.push(MapItem::All);
                    } else {
                        items.push(MapItem::Prop(self.name()?));
                    }
                } else if self.is_sym_at(1, ":") {
                    let k = self.name()?;
                    self.bump();
                    items.push(MapItem::Lit(k, self.expr()?));
                } else {
                    items.push(MapItem::Var(self.var()?));
                }
                if !self.accept_sym(",") {
                    break;
                }
            }
        }
        self.expect_sym("}")?;
        Ok(Expr::MapProjection(Box::new(base), items))
    }

    /// The body of `EXISTS { }`, `COUNT { }` or `COLLECT { }` after the
    /// brace: a query, or patterns with an optional `WHERE`.
    fn subquery_body(&mut self, kind: SubqueryKind) -> Result<Expr> {
        if self.is_clause_start() {
            let q = self.query_inner()?;
            self.expect_sym("}")?;
            return Ok(Expr::Subquery(kind, Box::new(q)));
        }
        let patterns = self.patterns()?;
        let where_ = if self.accept_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        self.expect_sym("}")?;
        if kind == SubqueryKind::Exists && where_.is_none() && patterns.len() == 1 {
            return Ok(Expr::Pattern(Box::new(
                patterns
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| unreachable!()),
            )));
        }
        Ok(Expr::Subquery(
            kind,
            Box::new(Query {
                explain: Explain::None,
                parts: vec![SingleQuery {
                    clauses: vec![Clause::Match {
                        optional: false,
                        patterns,
                        where_,
                    }],
                }],
                union_all: vec![],
            }),
        ))
    }

    /// Whether the tokens at the cursor start a relationship (after a node
    /// pattern): `-[`, `--`, `->`, `<-`.
    fn rel_follows(&self) -> bool {
        (self.is_sym("-")
            && (self.is_sym_at(1, "[") || self.is_sym_at(1, "-") || self.is_sym_at(1, ">")))
            || (self.is_sym("<") && self.is_sym_at(1, "-"))
    }

    #[allow(clippy::too_many_lines)] // one arm per atom
    fn atom(&mut self) -> Result<Expr> {
        let t = self.peek().clone();
        match t {
            Tok::Int(i) => {
                self.bump();
                Ok(Expr::Lit(Value::Int(i)))
            }
            Tok::Float(f) => {
                self.bump();
                Ok(Expr::Lit(Value::Float(f)))
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Lit(Value::String(s)))
            }
            Tok::Param(p) => {
                self.bump();
                Ok(Expr::Param(p))
            }
            Tok::Sym("(") => {
                // A pattern predicate, or a parenthesized expression.
                let save = self.pos;
                if self.node_pat().is_ok() && self.rel_follows() {
                    self.pos = save;
                    let p = self.pattern_body()?;
                    return Ok(Expr::Pattern(Box::new(p)));
                }
                self.pos = save;
                self.bump();
                let e = self.expr()?;
                self.expect_sym(")")?;
                Ok(e)
            }
            Tok::Sym("[") => {
                self.bump();
                // List comprehension: [x IN list WHERE .. | ..].
                if self.is_var_start() && self.is_kw_at(1, "IN") {
                    let var = self.var()?;
                    self.bump();
                    let list = self.expr()?;
                    let pred = if self.accept_kw("WHERE") {
                        Some(Box::new(self.expr()?))
                    } else {
                        None
                    };
                    let map = if self.accept_sym("|") {
                        Some(Box::new(self.expr()?))
                    } else {
                        None
                    };
                    self.expect_sym("]")?;
                    return Ok(Expr::ListComp {
                        var,
                        list: Box::new(list),
                        pred,
                        map,
                    });
                }
                let mut l = Vec::new();
                if !self.is_sym("]") {
                    l.push(self.expr()?);
                    while self.accept_sym(",") {
                        l.push(self.expr()?);
                    }
                }
                self.expect_sym("]")?;
                Ok(Expr::List(l))
            }
            Tok::Sym("{") => self.map_literal(),
            Tok::Ident(s) => {
                let up = s.to_ascii_uppercase();
                match up.as_str() {
                    "TRUE" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Bool(true)));
                    }
                    "FALSE" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Bool(false)));
                    }
                    "NULL" => {
                        self.bump();
                        return Ok(Expr::Lit(Value::Null));
                    }
                    "CASE" => return self.case(),
                    "EXISTS" | "COUNT" | "COLLECT" if self.is_sym_at(1, "{") => {
                        self.bump();
                        self.bump();
                        let kind = match up.as_str() {
                            "EXISTS" => SubqueryKind::Exists,
                            "COUNT" => SubqueryKind::Count,
                            _ => SubqueryKind::Collect,
                        };
                        return self.subquery_body(kind);
                    }
                    "ALL" | "ANY" | "NONE" | "SINGLE"
                        if self.is_sym_at(1, "(") && self.is_kw_at(3, "IN") =>
                    {
                        self.bump();
                        self.bump();
                        let var = self.var()?;
                        self.expect_kw("IN")?;
                        let list = self.expr()?;
                        self.expect_kw("WHERE")?;
                        let pred = self.expr()?;
                        self.expect_sym(")")?;
                        return Ok(Expr::Quantifier {
                            kind: up.to_ascii_lowercase(),
                            var,
                            list: Box::new(list),
                            pred: Box::new(pred),
                        });
                    }
                    _ => {}
                }
                // A function call: name(.name)* '('.
                let mut n = 1;
                while self.is_sym_at(n, ".") && matches!(self.peek_at(n + 1), Tok::Ident(_)) {
                    n += 2;
                }
                if self.is_sym_at(n, "(") {
                    let mut name = self.name()?;
                    while self.accept_sym(".") {
                        name.push('.');
                        name.push_str(&self.name()?);
                    }
                    self.expect_sym("(")?;
                    let lname = name.to_ascii_lowercase();
                    if lname == "count" && self.accept_sym("*") {
                        self.expect_sym(")")?;
                        return Ok(Expr::CountStar);
                    }
                    if lname == "exists" && self.is_sym("(") {
                        let save = self.pos;
                        if self.node_pat().is_ok() && self.rel_follows() {
                            self.pos = save;
                            let p = self.pattern_body()?;
                            self.expect_sym(")")?;
                            return Ok(Expr::Pattern(Box::new(p)));
                        }
                        self.pos = save;
                    }
                    let distinct = self.accept_kw("DISTINCT");
                    let mut args = Vec::new();
                    if !self.is_sym(")") {
                        args.push(self.expr()?);
                        while self.accept_sym(",") {
                            args.push(self.expr()?);
                        }
                    }
                    self.expect_sym(")")?;
                    return Ok(Expr::Func {
                        name: lname,
                        distinct,
                        args,
                    });
                }
                Ok(Expr::Var(self.var()?))
            }
            Tok::Quoted(s) => {
                self.bump();
                Ok(Expr::Var(s))
            }
            _ => Err(self.err("an expression")),
        }
    }

    fn case(&mut self) -> Result<Expr> {
        self.expect_kw("CASE")?;
        let subject = if self.is_kw("WHEN") {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut whens = Vec::new();
        while self.accept_kw("WHEN") {
            let w = self.expr()?;
            self.expect_kw("THEN")?;
            whens.push((w, self.expr()?));
        }
        if whens.is_empty() {
            return Err(self.err("WHEN"));
        }
        let else_ = if self.accept_kw("ELSE") {
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        self.expect_kw("END")?;
        Ok(Expr::Case {
            subject,
            whens,
            else_,
        })
    }
}

/// Parameters from a map of values (for callers holding JSON).
#[must_use]
pub fn params_from_json(j: &serde_json::Value) -> BTreeMap<String, Value> {
    match j {
        serde_json::Value::Object(o) => o
            .iter()
            .map(|(k, v)| (k.clone(), crate::value::from_json(v)))
            .collect(),
        _ => BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> Query {
        parse(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    #[test]
    fn parses_the_worked_example() {
        let q = ok(
            "MATCH (a:Person {email: $email})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product)
                    WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a
                    RETURN p.name AS product, count(DISTINCT f) AS buyers
                    ORDER BY buyers DESC LIMIT 5",
        );
        let Clause::Match {
            patterns, where_, ..
        } = &q.parts[0].clauses[0]
        else {
            panic!("match")
        };
        assert_eq!(patterns[0].steps.len(), 2);
        assert_eq!(patterns[0].steps[0].0.range, Some((Some(1), Some(2))));
        assert_eq!(patterns[0].steps[0].0.types, vec!["KNOWS"]);
        let w = where_.as_ref().expect("where");
        let mut found = false;
        w.walk(&mut |e| found |= matches!(e, Expr::Pattern(_)));
        assert!(found, "pattern predicate");
        let Clause::Return(p) = &q.parts[0].clauses[1] else {
            panic!("return")
        };
        assert_eq!(p.items[1].1, "buyers");
        assert!(matches!(p.items[1].0, Expr::Func { distinct: true, .. }));
        assert!(!p.order[0].1);
    }

    #[test]
    fn parses_the_clause_set() {
        for s in [
            "CREATE (a:Person:Admin {name: 'Ann', age: 30})-[:KNOWS {since: 2020}]->(b:Person {name: \"Bob\"})",
            "MERGE (n:L {k: 1}) ON CREATE SET n.c = 1 ON MATCH SET n.m = n.m + 1",
            "MATCH (n) WHERE id(n) = 1 SET n:X, n.a = 1, n += {b: 2} REMOVE n:Y, n.c",
            "MATCH (n:L) DETACH DELETE n",
            "MATCH p = shortestPath((a)-[*..5]-(b)) RETURN p, length(p)",
            "MATCH p = ALL SHORTEST (a)-[:R]->+(b) RETURN p",
            "MATCH (a)-[r:R|S*]->(b) RETURN DISTINCT b ORDER BY b.x SKIP 1 LIMIT 2",
            "MATCH (a)<-[:R]-(b)--(c) RETURN *",
            "UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN collect(x) AS xs",
            "RETURN CASE WHEN 1 < 2 THEN 'a' ELSE 'b' END AS c, [x IN range(1, 5) WHERE x % 2 = 0 | x * x] AS sq",
            "RETURN all(x IN [1,2] WHERE x > 0) AS a, -9223372036854775808 AS m, 0x1F AS h, 1.5e3 AS f",
            "MATCH (n:A|B&!C) RETURN n:A AS isA",
            "CALL db.labels() YIELD label RETURN label",
            "CALL db.labels",
            "MATCH (a) RETURN a UNION ALL MATCH (b) RETURN b",
            "EXPLAIN MATCH (n) RETURN n",
            "MATCH (a)-[:R]->{2,3}(b) RETURN b",
            "RETURN 'it''s' AS x",
        ] {
            if s.contains("'it''s'") {
                assert!(parse(s).is_err());
                continue;
            }
            ok(s);
        }
    }

    #[test]
    fn errors_are_39900_with_offsets() {
        for s in [
            "MATCH (n RETURN n",
            "RETURN 1 +",
            "MATCH (a)-[:R]-<(b) RETURN a",
            "FOO",
            "RETURN 'x",
        ] {
            let e = parse(s).expect_err(s);
            assert_eq!(e.code(), Some(SprigCode::GraphSyntax), "{s}");
            assert!(e.to_string().contains("offset"), "{e}");
        }
        let e = parse("CALL { RETURN 1 } IN TRANSACTIONS").expect_err("batches");
        assert_eq!(e.code(), Some(SprigCode::GraphUnsupported));
        let e = parse("WITH 1 + 1 RETURN 1").expect_err("alias");
        assert_eq!(e.code(), Some(SprigCode::GraphSyntax));
    }

    #[test]
    fn unaliased_return_items_keep_their_text() {
        let q = ok("MATCH (n) RETURN n.name, count(*), toUpper(n.name)");
        let Clause::Return(p) = &q.parts[0].clauses[1] else {
            panic!()
        };
        let names: Vec<&str> = p.items.iter().map(|(_, a)| a.as_str()).collect();
        assert_eq!(names, ["n.name", "count(*)", "toUpper(n.name)"]);
    }
}
