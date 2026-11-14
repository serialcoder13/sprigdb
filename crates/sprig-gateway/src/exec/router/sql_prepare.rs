//! SQL-level prepared statements of tenants with sharded or reference
//! tables: `PREPARE
//! name FROM 'text' | @var | expr`, `EXECUTE name [USING ...]`, `EXECUTE
//! IMMEDIATE ...` and `{DEALLOCATE | DROP} PREPARE name`.
//!
//! The gateway keeps them, as it keeps protocol prepared statements: the
//! router's parser doesn't read `PREPARE`, and a backend statement would
//! live on one connection while the session's statements move between
//! shards, clusters and a transaction's branches.
//!
//! ```text
//! PREPARE n FROM src   src's text (a literal, or evaluated on the backend);
//!                      prepared as COM_STMT_PREPARE would be (names,
//!                      privileges, row-level security, the shard's
//!                      metadata), then closed: errors come at PREPARE, as
//!                      in MariaDB. Kept with its `?` count and the
//!                      database current then. A failed PREPARE drops n.
//! EXECUTE n USING ...  the values (literals here, the rest in one SELECT on
//!                      the session), written into the text as literals the
//!                      router routes ([`sprig_router::ParamLit`]); the
//!                      text then runs as the client's own statement:
//!                      planned again with the layout and maps of the
//!                      moment, so a shard move never leaves it stale, and
//!                      refused only as that statement would be
//! DEALLOCATE n         forgets n
//! ```
//!
//! A name the gateway doesn't hold gets 1243 from the gateway: sent to the
//! backend, the command would pin the session's connection for good.
//! Tenants without sharded or reference tables keep MariaDB's own
//! statements.
//!
//! Differences from MariaDB: an expression with a parameter in the
//! select list is named after its value (`5+1`, not `?+1`; a bare `?` is
//! still named `?`), and a parameter's result column has the literal's
//! type.

use std::fmt::Write as _;

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::column_type as ct;
use sprig_router::ParamLit;
use sprig_sql::lex::{LexOptions, Token, TokenKind, lex, string_value};

use crate::exec::PassSession;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// A statement the gateway holds for a name.
#[derive(Debug, Clone)]
pub(crate) struct Held {
    /// The text, as the client wrote it (logical names).
    text: Bytes,
    /// Its `?` placeholders.
    params: usize,
    /// The logical database current at `PREPARE`: names bind there, as
    /// in MariaDB.
    db: Option<String>,
}

/// One SQL-level prepared-statement command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlPs {
    /// `PREPARE name FROM src`.
    Prepare { name: String, src: Src },
    /// `EXECUTE name [USING a, b]`.
    Execute { name: String, using: Vec<Src> },
    /// `EXECUTE IMMEDIATE src [USING a, b]`.
    Immediate { src: Src, using: Vec<Src> },
    /// `DEALLOCATE PREPARE name`, `DROP PREPARE name`.
    Deallocate { name: String },
}

/// An operand: its text, and the value when it is a plain literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Src {
    /// The expression as written.
    pub(crate) text: Vec<u8>,
    /// What a literal reads as without the backend.
    pub(crate) lit: Option<Lit>,
}

/// A literal operand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Lit {
    Null,
    /// A number as written.
    Number(String),
    /// A string's bytes (the connection's character set).
    Str(Vec<u8>),
}

fn significant(k: TokenKind) -> bool {
    !matches!(
        k,
        TokenKind::Whitespace
            | TokenKind::LineComment
            | TokenKind::BlockComment
            | TokenKind::Hint
            | TokenKind::InactiveExec
    )
}

fn word(src: &[u8], t: &Token, w: &str) -> bool {
    t.kind == TokenKind::Word && t.bytes(src).eq_ignore_ascii_case(w.as_bytes())
}

fn punct(src: &[u8], t: &Token, c: u8) -> bool {
    t.kind == TokenKind::Punct && t.bytes(src) == [c]
}

/// A statement name as written, unquoted (callers fold it: names are
/// case-insensitive).
fn name_of(src: &[u8], tok: &Token) -> Option<String> {
    let b = tok.bytes(src);
    let s = match tok.kind {
        TokenKind::Word => String::from_utf8_lossy(b).into_owned(),
        TokenKind::QuotedIdent if b.len() >= 2 => {
            let q = b[0];
            let inner = &b[1..b.len() - 1];
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
            String::from_utf8_lossy(&v).into_owned()
        }
        _ => return None,
    };
    Some(s)
}

/// The operand spanning significant tokens `toks` of `src`.
fn src_of(src: &[u8], toks: &[Token], lex: LexOptions) -> Option<Src> {
    let (first, last) = (toks.first()?, toks.last()?);
    let text = src.get(first.start as usize..last.end as usize)?.to_vec();
    let lit = match toks {
        [t] if t.kind == TokenKind::String => {
            string_value(t.bytes(src), lex.no_backslash_escapes).map(Lit::Str)
        }
        [t] if t.kind == TokenKind::Number => Some(Lit::Number(
            String::from_utf8_lossy(t.bytes(src)).into_owned(),
        )),
        [t] if word(src, t, "NULL") => Some(Lit::Null),
        _ => None,
    };
    Some(Src { text, lit })
}

/// `toks` split at the commas outside parentheses; `None` for an empty
/// item.
fn items(src: &[u8], toks: &[Token], lex: LexOptions) -> Option<Vec<Src>> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut from = 0;
    for (i, t) in toks.iter().enumerate() {
        if punct(src, t, b'(') {
            depth += 1;
        } else if punct(src, t, b')') {
            depth -= 1;
        } else if depth == 0 && punct(src, t, b',') {
            out.push(src_of(src, &toks[from..i], lex)?);
            from = i + 1;
        }
    }
    out.push(src_of(src, &toks[from..], lex)?);
    Some(out)
}

/// The first `USING` outside parentheses.
fn using_at(src: &[u8], toks: &[Token]) -> Option<usize> {
    let mut depth = 0i32;
    for (i, t) in toks.iter().enumerate() {
        if punct(src, t, b'(') {
            depth += 1;
        } else if punct(src, t, b')') {
            depth -= 1;
        } else if depth == 0 && word(src, t, "USING") {
            return Some(i);
        }
    }
    None
}

/// Whether `text` starts with a word of the commands this module reads
/// (cheap: before any lexing).
pub(crate) fn candidate(text: &[u8]) -> bool {
    let t = text.trim_ascii_start();
    let starts = |w: &[u8]| t.len() > w.len() && t[..w.len()].eq_ignore_ascii_case(w);
    starts(b"PREPARE") || starts(b"EXECUTE") || starts(b"DEALLOCATE") || starts(b"DROP")
}

/// Reads one SQL-level prepared-statement command; `None` for any other
/// statement (or one written in a way this doesn't read, such as inside
/// an executable comment: it goes on as before).
pub(crate) fn parse(text: &[u8], opts: LexOptions) -> Option<SqlPs> {
    if !candidate(text) {
        return None;
    }
    let all = lex(text, &opts).ok()?;
    if all
        .iter()
        .any(|t| matches!(t.kind, TokenKind::ExecOpen | TokenKind::Unterminated))
    {
        return None;
    }
    let mut toks: Vec<Token> = all.into_iter().filter(|t| significant(t.kind)).collect();
    while toks.last().is_some_and(|t| punct(text, t, b';')) {
        toks.pop();
    }
    let t = &toks;
    if t.len() >= 4 && word(text, &t[0], "PREPARE") && word(text, &t[2], "FROM") {
        let name = name_of(text, &t[1])?;
        let src = src_of(text, &t[3..], opts)?;
        return Some(SqlPs::Prepare { name, src });
    }
    if t.len() == 3
        && (word(text, &t[0], "DEALLOCATE") || word(text, &t[0], "DROP"))
        && word(text, &t[1], "PREPARE")
    {
        return Some(SqlPs::Deallocate {
            name: name_of(text, &t[2])?,
        });
    }
    if t.len() >= 2 && word(text, &t[0], "EXECUTE") {
        if word(text, &t[1], "IMMEDIATE") && t.len() >= 3 {
            let rest = &t[2..];
            return Some(match using_at(text, rest) {
                Some(u) => SqlPs::Immediate {
                    src: src_of(text, &rest[..u], opts)?,
                    using: items(text, &rest[u + 1..], opts)?,
                },
                None => SqlPs::Immediate {
                    src: src_of(text, rest, opts)?,
                    using: Vec::new(),
                },
            });
        }
        let name = name_of(text, &t[1])?;
        let using = match t.get(2) {
            None => Vec::new(),
            Some(u) if word(text, u, "USING") && t.len() > 3 => items(text, &t[3..], opts)?,
            Some(_) => return None,
        };
        return Some(SqlPs::Execute { name, using });
    }
    None
}

/// The `?` placeholders of `text`.
pub(crate) fn param_count(text: &[u8], opts: LexOptions) -> usize {
    lex(text, &opts)
        .map(|t| t.iter().filter(|t| t.kind == TokenKind::Param).count())
        .unwrap_or(0)
}

/// Words that end a select list.
const LIST_END: &[&str] = &[
    "FROM",
    "INTO",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "FOR",
    "LOCK",
    "PROCEDURE",
];

/// `text` with its `?` placeholders replaced by `params` in order; a
/// select-list item that is only a `?` keeps MariaDB's name for it (an
/// alias of a backquoted question mark). `None` when the counts differ.
pub(crate) fn render(text: &[u8], params: &[ParamLit], opts: LexOptions) -> Option<Vec<u8>> {
    let toks = lex(text, &opts).ok()?;
    let sig: Vec<usize> = (0..toks.len())
        .filter(|&i| significant(toks[i].kind))
        .collect();
    // Per parenthesis depth: inside a select list.
    let mut frames: Vec<bool> = vec![false];
    let mut select_item = vec![false; toks.len()];
    for (j, &i) in sig.iter().enumerate() {
        let t = &toks[i];
        if punct(text, t, b'(') {
            frames.push(false);
        } else if punct(text, t, b')') {
            if frames.len() > 1 {
                frames.pop();
            }
        } else if word(text, t, "SELECT") {
            if let Some(f) = frames.last_mut() {
                *f = true;
            }
        } else if t.kind == TokenKind::Word && LIST_END.iter().any(|w| word(text, t, w)) {
            if let Some(f) = frames.last_mut() {
                *f = false;
            }
        } else if t.kind == TokenKind::Param && frames.last().copied().unwrap_or(false) {
            let prev = j.checked_sub(1).map(|p| &toks[sig[p]]);
            let next = sig.get(j + 1).map(|&n| &toks[n]);
            let starts = prev.is_some_and(|p| {
                punct(text, p, b',')
                    || ["SELECT", "DISTINCT", "ALL", "DISTINCTROW"]
                        .iter()
                        .any(|w| word(text, p, w))
            });
            let ends = next.is_none_or(|n| {
                punct(text, n, b',')
                    || punct(text, n, b')')
                    || punct(text, n, b';')
                    || (n.kind == TokenKind::Word && LIST_END.iter().any(|w| word(text, n, w)))
            });
            select_item[i] = starts && ends;
        }
    }
    let mut out = Vec::with_capacity(text.len() + 16 * params.len());
    let mut next = params.iter();
    for (i, t) in toks.iter().enumerate() {
        if t.kind != TokenKind::Param {
            out.extend_from_slice(t.bytes(text));
            continue;
        }
        let lit = next.next()?.sql();
        // Keep tokens apart: `a-?` with -5 must not become `a--5`.
        let glue = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'-' | b'@');
        if out.last().is_some_and(|&b| glue(b)) {
            out.push(b' ');
        }
        out.extend_from_slice(lit.as_bytes());
        if select_item[i] {
            out.extend_from_slice(b" AS `?`");
        }
        if text
            .get(t.end as usize)
            .is_some_and(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
        {
            out.push(b' ');
        }
    }
    next.next().is_none().then_some(out)
}

/// A literal operand as a parameter value.
fn lit_param(l: &Lit, charset: &str) -> ParamLit {
    match l {
        Lit::Null => ParamLit::Null,
        Lit::Number(n) => n
            .parse::<i128>()
            .map_or_else(|_| ParamLit::Raw(n.clone()), ParamLit::Int),
        Lit::Str(b) => ParamLit::Str {
            bytes: b.clone(),
            charset: charset.to_owned(),
        },
    }
}

fn unhex(h: &[u8]) -> Option<Vec<u8>> {
    if !h.len().is_multiple_of(2) {
        return None;
    }
    h.chunks(2)
        .map(|p| {
            let s = std::str::from_utf8(p).ok()?;
            u8::from_str_radix(s, 16).ok()
        })
        .collect()
}

/// A value the backend computed (`e, HEX(e), CHARSET(e)`) as a parameter.
pub(crate) fn value_param(
    col_type: u8,
    value: Option<&[u8]>,
    hex: Option<&[u8]>,
    charset: Option<&[u8]>,
) -> ParamLit {
    let Some(v) = value else {
        return ParamLit::Null;
    };
    let text = String::from_utf8_lossy(v).into_owned();
    match col_type {
        ct::TINY | ct::SHORT | ct::LONG | ct::LONGLONG | ct::INT24 | ct::YEAR => text
            .parse::<i128>()
            .map_or_else(|_| ParamLit::Raw(text.clone()), ParamLit::Int),
        ct::DECIMAL | ct::NEWDECIMAL => ParamLit::Raw(text),
        ct::FLOAT | ct::DOUBLE => text
            .parse::<f64>()
            .map_or(ParamLit::Raw(text), ParamLit::Double),
        ct::DATE | ct::NEWDATE => ParamLit::Raw(format!("DATE'{text}'")),
        ct::DATETIME | ct::DATETIME2 | ct::TIMESTAMP | ct::TIMESTAMP2 => {
            ParamLit::Raw(format!("TIMESTAMP'{text}'"))
        }
        ct::TIME | ct::TIME2 => ParamLit::Raw(format!("TIME'{text}'")),
        _ => {
            let bytes = hex.and_then(unhex).unwrap_or_else(|| v.to_vec());
            match charset.map(|c| String::from_utf8_lossy(c).to_ascii_lowercase()) {
                Some(c) if c == "binary" => ParamLit::Bytes(bytes),
                Some(c) if !c.is_empty() && c.bytes().all(|b| b.is_ascii_alphanumeric()) => {
                    ParamLit::Str { bytes, charset: c }
                }
                _ => ParamLit::Str {
                    bytes,
                    charset: "utf8mb4".to_owned(),
                },
            }
        }
    }
}

/// 1243, as MariaDB answers a name it doesn't hold. The gateway answers it
/// itself: sent to the backend, the command would pin the session's
/// connection (SQL-level prepared statements are untracked state), and a
/// pinned session loses gathered reads among other things.
fn unknown(name: &str, cmd: &str) -> SprigError {
    SprigError::standard(
        1243,
        *b"HY000",
        format!("Unknown prepared statement handler ({name}) given to {cmd}"),
    )
}

/// 1210, as MariaDB answers a wrong number of `USING` values.
fn wrong_arguments() -> SprigError {
    SprigError::standard(1210, *b"HY000", "Incorrect arguments to EXECUTE")
}

impl PassSession {
    /// The session's `character_set_client`.
    fn client_charset(&self) -> String {
        self.bs
            .as_ref()
            .and_then(|b| b.vars().vars.get("character_set_client").cloned())
            .unwrap_or_else(|| "utf8mb4".to_owned())
    }

    /// The values of `items`: literals here, the rest from one `SELECT`
    /// on the session.
    async fn using_values(&mut self, cx: &mut ExecCtx<'_>, items: &[Src]) -> Result<Vec<ParamLit>> {
        let charset = self.client_charset();
        let mut out: Vec<Option<ParamLit>> = items
            .iter()
            .map(|s| s.lit.as_ref().map(|l| lit_param(l, &charset)))
            .collect();
        let asked: Vec<usize> = (0..items.len()).filter(|&i| out[i].is_none()).collect();
        if !asked.is_empty() {
            let mut sql = String::from("SELECT ");
            for (n, &i) in asked.iter().enumerate() {
                let e = String::from_utf8_lossy(&items[i].text);
                if n > 0 {
                    sql.push_str(", ");
                }
                let _ = write!(sql, "({e}), HEX(({e})), CHARSET(({e}))");
            }
            let mut c = Box::pin(self.run_checked(cx, &sql, false)).await?;
            let row = c.rows.pop().unwrap_or_default();
            for (n, &i) in asked.iter().enumerate() {
                let at = |k: usize| row.get(3 * n + k).and_then(Option::as_deref);
                out[i] = Some(value_param(
                    c.types.get(3 * n).copied().unwrap_or(ct::VAR_STRING),
                    at(0),
                    at(1),
                    at(2),
                ));
            }
        }
        Ok(out
            .into_iter()
            .map(|p| p.unwrap_or(ParamLit::Null))
            .collect())
    }

    /// The text `PREPARE` or `EXECUTE IMMEDIATE` names: a string literal
    /// as written, else the expression's value from the backend, in the
    /// connection's character set.
    async fn source_text(&mut self, cx: &mut ExecCtx<'_>, src: &Src) -> Result<Vec<u8>> {
        if let Some(Lit::Str(b)) = &src.lit {
            return Ok(b.clone());
        }
        let e = String::from_utf8_lossy(&src.text);
        let mut c = Box::pin(self.run_checked(cx, &format!("SELECT ({e})"), false)).await?;
        match c.rows.pop().and_then(|r| r.into_iter().next()).flatten() {
            Some(v) => Ok(v),
            None => Err(SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax; check the manual that corresponds to \
                 your MariaDB server version for the right syntax to use near 'NULL' at line 1",
            )),
        }
    }

    /// `text` checked as `COM_STMT_PREPARE` checks it (and closed): the
    /// held statement, or the error MariaDB's `PREPARE` would give.
    async fn hold(&mut self, cx: &mut ExecCtx<'_>, text: Vec<u8>) -> Result<Held> {
        let text = Bytes::from(text);
        let summary = Box::pin(self.tenant_prepare(cx, text.clone())).await?;
        if let Some(meta) = summary.prepared {
            self.close_cursor(meta.stmt);
            if self.stmts.remove(meta.stmt).is_some() {
                metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
                    .decrement(1.0);
            }
        }
        let (lex, _) = self.lex_opts();
        Ok(Held {
            params: param_count(&text, lex),
            text,
            db: self.current_logical(),
        })
    }

    /// Runs `held` with `using`'s values as the client's own statement,
    /// in the database it was prepared in.
    #[allow(clippy::too_many_arguments)]
    async fn run_held(
        &mut self,
        cx: &mut ExecCtx<'_>,
        held: &Held,
        using: &[Src],
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        if using.len() != held.params {
            return self.fail(out, &wrong_arguments()).await;
        }
        let params = match self.using_values(cx, using).await {
            Ok(p) => p,
            Err(e) => {
                let e = self.map_error(e);
                return self.fail(out, &e).await;
            }
        };
        let (lex, _) = self.lex_opts();
        let Some(text) = render(&held.text, &params, lex) else {
            return self.fail(out, &wrong_arguments()).await;
        };
        // Names bind in the database of the PREPARE, as in MariaDB.
        let now = self.current_logical();
        let switch = match (&held.db, &now) {
            (Some(then), Some(now)) if then != now => Some((then.clone(), now.clone())),
            _ => None,
        };
        if let Some((then, _)) = &switch {
            let sql = format!("USE {}", crate::backend::state::quote_ident(then));
            if let Err(e) = Box::pin(self.run_checked(cx, &sql, false)).await {
                return self.fail(out, &e).await;
            }
        }
        let r = Box::pin(self.tenant_text(
            cx,
            &Bytes::from(text),
            &QueryAttrs::default(),
            more,
            format,
            out,
        ))
        .await;
        if let Some((_, back)) = &switch {
            let sql = format!("USE {}", crate::backend::state::quote_ident(back));
            Box::pin(self.run_checked(cx, &sql, false)).await?;
        }
        r
    }

    async fn ps_ok(&mut self, out: &mut dyn ResultSink, more: bool, info: &str) -> Result<Step> {
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let mut ok = OkPacket::new(status);
        ok.info = Bytes::copy_from_slice(info.as_bytes());
        out.ok(ok).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    /// A SQL-level prepared-statement command of a tenant session (see the
    /// module docs): `None` when it isn't one, or goes on as before.
    pub(super) async fn sql_prepared(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !candidate(original) {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        let Some(ps) = parse(original, lex) else {
            return Ok(None);
        };
        let sharded = self.router_layout().await.is_some();
        let step = match ps {
            SqlPs::Prepare { name, src } => {
                // MariaDB drops the old statement first, whatever comes.
                self.router.sql_ps.remove(&name.to_lowercase());
                if !sharded {
                    return Ok(None);
                }
                let held = match self.source_text(cx, &src).await {
                    Ok(t) => self.hold(cx, t).await,
                    Err(e) => Err(e),
                };
                match held {
                    Ok(h) => {
                        self.router.sql_ps.insert(name.to_lowercase(), h);
                        self.ps_ok(out, more, "Statement prepared").await?
                    }
                    Err(e) => {
                        let e = self.map_error(e);
                        self.fail(out, &e).await?
                    }
                }
            }
            SqlPs::Execute { name, using } => {
                match self.router.sql_ps.get(&name.to_lowercase()).cloned() {
                    Some(held) => self.run_held(cx, &held, &using, more, format, out).await?,
                    None if sharded => self.fail(out, &unknown(&name, "EXECUTE")).await?,
                    None => return Ok(None),
                }
            }
            SqlPs::Immediate { src, using } => {
                if !sharded {
                    return Ok(None);
                }
                let held = match self.source_text(cx, &src).await {
                    Ok(t) => self.hold(cx, t).await,
                    Err(e) => Err(e),
                };
                match held {
                    Ok(h) => self.run_held(cx, &h, &using, more, format, out).await?,
                    Err(e) => {
                        let e = self.map_error(e);
                        self.fail(out, &e).await?
                    }
                }
            }
            SqlPs::Deallocate { name } => {
                if self.router.sql_ps.remove(&name.to_lowercase()).is_some() {
                    self.ps_ok(out, more, "").await?
                } else if sharded {
                    self.fail(out, &unknown(&name, "DEALLOCATE PREPARE"))
                        .await?
                } else {
                    return Ok(None);
                }
            }
        };
        Ok(Some(step))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lx() -> LexOptions {
        LexOptions::mariadb()
    }

    fn p(s: &str) -> Option<SqlPs> {
        parse(s.as_bytes(), lx())
    }

    fn text(s: &Src) -> String {
        String::from_utf8_lossy(&s.text).into_owned()
    }

    #[test]
    fn reads_prepare_execute_and_deallocate() {
        let Some(SqlPs::Prepare { name, src }) = p("PREPARE s1 FROM 'SELECT * FROM t WHERE a = ?'")
        else {
            panic!()
        };
        assert_eq!(name, "s1");
        assert_eq!(
            src.lit,
            Some(Lit::Str(b"SELECT * FROM t WHERE a = ?".to_vec()))
        );
        let Some(SqlPs::Prepare { name, src }) = p("prepare `My S` from @q;") else {
            panic!()
        };
        assert_eq!(name, "My S");
        assert_eq!(text(&src), "@q");
        assert_eq!(src.lit, None);
        let Some(SqlPs::Prepare { src, .. }) = p("PREPARE s FROM CONCAT('SELECT ', @c)") else {
            panic!()
        };
        assert_eq!(text(&src), "CONCAT('SELECT ', @c)");
        // Escapes and doubled quotes are undone.
        let Some(SqlPs::Prepare { src, .. }) = p(r"PREPARE s FROM 'SELECT ''a\'b'''") else {
            panic!()
        };
        assert_eq!(src.lit, Some(Lit::Str(b"SELECT 'a'b'".to_vec())));

        let Some(SqlPs::Execute { name, using }) = p("EXECUTE S1 USING @a, 5, 'x', NULL, f(1, 2)")
        else {
            panic!()
        };
        assert_eq!(name, "S1");
        let t: Vec<String> = using.iter().map(text).collect();
        assert_eq!(t, ["@a", "5", "'x'", "NULL", "f(1, 2)"]);
        assert_eq!(using[0].lit, None);
        assert_eq!(using[1].lit, Some(Lit::Number("5".into())));
        assert_eq!(using[2].lit, Some(Lit::Str(b"x".to_vec())));
        assert_eq!(using[3].lit, Some(Lit::Null));
        assert_eq!(using[4].lit, None);
        assert_eq!(
            p("EXECUTE s"),
            Some(SqlPs::Execute {
                name: "s".into(),
                using: vec![]
            })
        );
        assert_eq!(
            p("DEALLOCATE PREPARE s"),
            Some(SqlPs::Deallocate { name: "s".into() })
        );
        assert_eq!(
            p("drop prepare `s`"),
            Some(SqlPs::Deallocate { name: "s".into() })
        );
        let Some(SqlPs::Immediate { src, using }) =
            p("EXECUTE IMMEDIATE 'SELECT ? + ?' USING 1, @b")
        else {
            panic!()
        };
        assert_eq!(src.lit, Some(Lit::Str(b"SELECT ? + ?".to_vec())));
        assert_eq!(using.len(), 2);
        let Some(SqlPs::Immediate { src, using }) = p("EXECUTE IMMEDIATE CONCAT('SELECT ', 1)")
        else {
            panic!()
        };
        assert_eq!(text(&src), "CONCAT('SELECT ', 1)");
        assert!(using.is_empty());
    }

    #[test]
    fn leaves_other_statements_alone() {
        for s in [
            "SELECT 1",
            "DROP TABLE t",
            "DROP PREPARE",
            "EXECUTE",
            "EXECUTE s USING",
            "EXECUTE s junk",
            "PREPARE s",
            "PREPARE s FROM",
            "PREPAREX s FROM 'x'",
            "/*!100000 PREPARE s FROM 'SELECT 1' */",
            "PREPARE s FROM 'unterminated",
            "XA PREPARE 'x'",
        ] {
            assert_eq!(p(s), None, "{s}");
        }
    }

    fn r(s: &str, params: &[ParamLit]) -> Option<String> {
        render(s.as_bytes(), params, lx()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn renders_values_where_placeholders_are() {
        assert_eq!(
            r(
                "SELECT * FROM t WHERE a = ? AND b IN (?, ?) AND c = '?' -- ?\n",
                &[
                    ParamLit::Int(5),
                    ParamLit::Str {
                        bytes: b"ab".to_vec(),
                        charset: "utf8mb4".into()
                    },
                    ParamLit::Null,
                ],
            )
            .as_deref(),
            Some(
                "SELECT * FROM t WHERE a = 5 AND b IN (_utf8mb4 X'6162', NULL) AND c = '?' -- ?\n"
            )
        );
        // Counts must agree.
        assert_eq!(r("SELECT ?", &[]), None);
        assert_eq!(r("SELECT 1", &[ParamLit::Int(1)]), None);
        // Tokens stay apart.
        assert_eq!(
            r(
                "SELECT a-? FROM t WHERE b=?AND c",
                &[ParamLit::Int(-5), ParamLit::Int(2)]
            )
            .as_deref(),
            Some("SELECT a- -5 FROM t WHERE b=2 AND c")
        );
        assert_eq!(
            r(
                "INSERT INTO t VALUES (?, ?)",
                &[ParamLit::Int(1), ParamLit::Raw("1.50".into())]
            )
            .as_deref(),
            Some("INSERT INTO t VALUES (1, 1.50)")
        );
    }

    #[test]
    fn a_bare_placeholder_in_the_select_list_keeps_its_name() {
        let one = [ParamLit::Int(7)];
        let two = [ParamLit::Int(7), ParamLit::Int(8)];
        assert_eq!(r("SELECT ?", &one).as_deref(), Some("SELECT 7 AS `?`"));
        assert_eq!(
            r("SELECT ?, ? FROM t", &two).as_deref(),
            Some("SELECT 7 AS `?`, 8 AS `?` FROM t")
        );
        assert_eq!(
            r("SELECT DISTINCT ? FROM t WHERE a = ?", &two).as_deref(),
            Some("SELECT DISTINCT 7 AS `?` FROM t WHERE a = 8")
        );
        // Not bare: an expression, an aliased item, an argument, a value
        // list, LIMIT.
        assert_eq!(r("SELECT ?+1", &one).as_deref(), Some("SELECT 7+1"));
        assert_eq!(r("SELECT ? AS y", &one).as_deref(), Some("SELECT 7 AS y"));
        assert_eq!(r("SELECT f(?)", &one).as_deref(), Some("SELECT f(7)"));
        assert_eq!(
            r("SELECT a FROM t LIMIT ?, ?", &two).as_deref(),
            Some("SELECT a FROM t LIMIT 7, 8")
        );
        assert_eq!(
            r("SELECT a FROM t WHERE b IN (SELECT ? FROM u)", &one).as_deref(),
            Some("SELECT a FROM t WHERE b IN (SELECT 7 AS `?` FROM u)")
        );
        assert_eq!(
            r("UPDATE t SET a = ?, b = ?", &two).as_deref(),
            Some("UPDATE t SET a = 7, b = 8")
        );
    }

    #[test]
    fn backend_values_become_literals() {
        assert_eq!(value_param(ct::LONGLONG, None, None, None), ParamLit::Null);
        assert_eq!(
            value_param(ct::LONGLONG, Some(b"-42"), Some(b"FF"), Some(b"binary")),
            ParamLit::Int(-42)
        );
        assert_eq!(
            value_param(ct::NEWDECIMAL, Some(b"1.50"), None, None),
            ParamLit::Raw("1.50".into())
        );
        assert_eq!(
            value_param(ct::DOUBLE, Some(b"0.25"), None, None),
            ParamLit::Double(0.25)
        );
        assert_eq!(
            value_param(ct::DATETIME, Some(b"2026-11-13 01:02:03"), None, None),
            ParamLit::Raw("TIMESTAMP'2026-11-13 01:02:03'".into())
        );
        assert_eq!(
            value_param(ct::VAR_STRING, Some(b"ab"), Some(b"6162"), Some(b"utf8mb4")),
            ParamLit::Str {
                bytes: b"ab".to_vec(),
                charset: "utf8mb4".into()
            }
        );
        // The variable's own bytes, in its own character set.
        assert_eq!(
            value_param(ct::VAR_STRING, Some(b"?"), Some(b"E9"), Some(b"latin1")),
            ParamLit::Str {
                bytes: vec![0xE9],
                charset: "latin1".into()
            }
        );
        assert_eq!(
            value_param(
                ct::LONG_BLOB,
                Some(b"\x00\x01"),
                Some(b"0001"),
                Some(b"binary")
            ),
            ParamLit::Bytes(vec![0, 1])
        );
        assert_eq!(
            lit_param(&Lit::Number("1e3".into()), "utf8mb4"),
            ParamLit::Raw("1e3".into())
        );
        assert_eq!(
            lit_param(&Lit::Number("12".into()), "utf8mb4"),
            ParamLit::Int(12)
        );
    }

    #[test]
    fn counts_placeholders_outside_strings_and_comments() {
        assert_eq!(
            param_count(b"SELECT ?, '?', `?` /* ? */ FROM t WHERE a = ?", lx()),
            2
        );
    }
}
