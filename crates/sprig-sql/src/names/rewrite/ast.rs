//! The AST path: schema positions from sqlparser-rs, for each
//! object name in the AST.
//!
//! For each statement of a kind sqlparser-rs models, the walker's split
//! gives the statement's text. It is parsed with `MySqlDialect` from a
//! **parse view**: a copy with the same characters on the same lines,
//! where comments, hints and executable-comment markers are blanked,
//! string contents are replaced by `x` (so backslash rules can't differ),
//! and `ANSI_QUOTES` identifiers are backquoted. The AST's identifier spans
//! (line and column) therefore point at the original tokens.
//!
//! A visitor collects the database parts of object names in relation
//! positions, of `db.t.c` columns, `db.f()` calls and `db.t.*` wildcards,
//! and the targets of `DROP`, multi-table `DELETE`, `REFERENCES`, `LIKE`
//! and `RENAME TO`. Every maximal dotted name in the text must be accounted
//! for by the AST (as a database-qualified name, or a `t.c` or `t.*` it
//! saw); if one is not, or the statement doesn't parse, the walker's answer
//! stands ([`Engine::Auto`]) or the statement fails with 39051
//! ([`Engine::Ast`]).
//!
//! For the DDL kinds it models (`CREATE TABLE`, `CREATE VIEW`, `ALTER
//! TABLE`, `TRUNCATE`, `CREATE INDEX`, `DROP TABLE` and `DROP VIEW`), the
//! AST also gives the statement's [`DdlTarget`]; it must equal the
//! walker's for the engines to agree.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::ops::Range;

use sprig_common::names::quote;
use sqlparser::ast::{
    AlterTableOperation, AssignmentTarget, ColumnOption, CreateTableLikeKind, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart, OnInsert,
    RenameTableNameKind, Select, SelectItem, SelectItemQualifiedWildcardKind, Statement,
    TableConstraint, Visit, Visitor,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::walker::Out;
use super::{Edit, EditKind, Engine, RewriteCtx, StmtKind, SystemRef, SystemSchema};
use crate::lex::{Token, TokenKind, ident_value};
use crate::names::NameError;
use crate::names::ddl::{AlterOp, DdlKind, DdlName, DdlTarget};
use crate::names::map::Purpose;

type R<T = ()> = Result<T, NameError>;

/// Runs the AST path over the eligible statements in `out`.
pub(super) fn apply(src: &[u8], tokens: &[Token], ctx: &RewriteCtx<'_>, out: &mut Out) -> R {
    let strict = ctx.engine == Engine::Ast;
    let unsupported = |why: &str| NameError::Unsupported(format!("AST path: {why}"));
    let Ok(text) = std::str::from_utf8(src) else {
        return if strict {
            Err(unsupported("text is not UTF-8"))
        } else {
            Ok(())
        };
    };
    for si in 0..out.statements.len() {
        let st = &out.statements[si];
        let purpose = match st.kind {
            StmtKind::Select => Purpose::Read,
            StmtKind::Insert | StmtKind::Replace | StmtKind::Update | StmtKind::Delete => {
                Purpose::Write
            }
            StmtKind::Ddl => Purpose::Ddl,
            _ if strict => return Err(unsupported("statement kind not modelled")),
            _ => continue,
        };
        let range = st.range.clone();
        // Without a dotted name there is no schema position for the AST to
        // find (database positions are the walker's), so skip the parse.
        if !strict && !has_dotted_name(src, tokens, &range) {
            continue;
        }
        let current = out.currents.get(si).cloned().flatten();
        let Some((found, ast_ddl)) =
            schema_positions(text, tokens, range.clone(), current.as_deref())
        else {
            if strict {
                return Err(unsupported(
                    "sqlparser-rs could not account for the statement",
                ));
            }
            continue;
        };
        // The walker's schema positions in this statement.
        let mut walker: Vec<Range<usize>> = out
            .edits
            .iter()
            .filter(|e| matches!(e.kind, EditKind::Schema { .. }) && contains(&range, &e.range))
            .map(|e| e.range.clone())
            .chain(st.system_refs.iter().map(|r| r.range.clone()))
            .collect();
        walker.sort_by_key(|r| r.start);
        walker.dedup();
        if strict && st.kind == StmtKind::Ddl && ast_ddl.is_none() {
            return Err(unsupported("sqlparser-rs could not classify the DDL"));
        }
        let ddl_agree = ast_ddl == st.ddl;
        let agree = walker == found;
        if !agree {
            replace_schema_edits(src, ctx, out, si, &range, &found, purpose)?;
        }
        let st = &mut out.statements[si];
        st.engine = Engine::Ast;
        st.engines_agree = agree && ddl_agree;
        if ast_ddl.is_some() {
            st.ddl = ast_ddl;
        }
    }
    Ok(())
}

/// Whether the statement at `range` has an identifier followed by `.`.
fn has_dotted_name(src: &[u8], tokens: &[Token], range: &Range<usize>) -> bool {
    let mut prev_ident = false;
    for t in tokens.iter().filter(|t| {
        !t.kind.is_trivia() && range.start <= t.start as usize && t.end as usize <= range.end
    }) {
        if prev_ident && t.kind == TokenKind::Punct && src[t.start as usize] == b'.' {
            return true;
        }
        prev_ident = t.kind.is_ident_like();
    }
    false
}

fn contains(outer: &Range<usize>, inner: &Range<usize>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

/// Replaces the walker's schema edits and system references in statement
/// `si` by the AST's positions.
fn replace_schema_edits(
    src: &[u8],
    ctx: &RewriteCtx<'_>,
    out: &mut Out,
    si: usize,
    range: &Range<usize>,
    found: &[Range<usize>],
    purpose: Purpose,
) -> R {
    out.edits
        .retain(|e| !(matches!(e.kind, EditKind::Schema { .. }) && contains(range, &e.range)));
    let mut refs = Vec::new();
    for r in found {
        let raw = ident_value(&src[r.clone()]);
        let name = std::str::from_utf8(&raw)
            .map_err(|_| NameError::UnknownDatabase(String::from_utf8_lossy(&raw).into_owned()))?;
        if let Some(schema) = SystemSchema::parse(name) {
            refs.push(SystemRef {
                schema,
                object: None,
                range: r.clone(),
            });
            continue;
        }
        let key = if ctx.lower_case_table_names {
            name.to_lowercase()
        } else {
            name.to_owned()
        };
        let schema = ctx.map.physical(&key, purpose)?.single().ok_or_else(|| {
            NameError::Unsupported(format!(
                "database '{key}' stands for several schemas in one statement"
            ))
        })?;
        out.edits.push(Edit {
            range: r.clone(),
            kind: EditKind::Schema {
                logical: key,
                schema,
            },
            replacement: quote(&schema.to_string()),
        });
    }
    out.statements[si].system_refs = refs;
    Ok(())
}

/// The byte ranges of the database tokens in the statement at `range`, by
/// the AST, and its DDL target (unqualified names in `current`), or `None`
/// when sqlparser-rs can't parse it or doesn't account for every dotted
/// name.
fn schema_positions(
    text: &str,
    tokens: &[Token],
    range: Range<usize>,
    current: Option<&str>,
) -> Option<(Vec<Range<usize>>, Option<DdlTarget>)> {
    let toks: Vec<Token> = tokens
        .iter()
        .copied()
        .filter(|t| t.start as usize >= range.start && t.end as usize <= range.end)
        .collect();
    let significant = toks.iter().filter(|t| !t.kind.is_trivia()).count();
    if significant > MAX_AST_TOKENS || nesting(text.as_bytes(), &toks) > MAX_AST_NESTING {
        return None;
    }
    let view = parse_view(text, &toks)?;
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(PARSER_RECURSION_LIMIT)
        .try_with_sql(&view)
        .ok()?
        .parse_statements()
        .ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let stmt = stmts.pop()?;
    if !modelled(&stmt) {
        return None;
    }
    let mut v = Collect::default();
    let _ = stmt.visit(&mut v);
    v.statement(&stmt);

    // (line, column) of each identifier token, as sqlparser-rs counts them.
    let base = range.start;
    let mut at: HashMap<(u64, u64), usize> = HashMap::new();
    let (mut line, mut col, mut pos) = (1u64, 1u64, base);
    let sig: Vec<usize> = (0..toks.len())
        .filter(|&k| !toks[k].kind.is_trivia())
        .collect();
    for (k, t) in toks.iter().enumerate() {
        let start = t.start as usize;
        for ch in text[pos..start].chars() {
            if ch == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        pos = start;
        if t.kind.is_ident_like() {
            at.insert((line, col), k);
        }
    }
    let tok_of = |s: &(u64, u64)| at.get(s).copied();

    let mut found: Vec<Range<usize>> = Vec::new();
    for s in &v.schemas {
        found.push(toks[tok_of(s)?].range());
    }
    found.sort_by_key(|r| r.start);
    found.dedup();

    // Every maximal dotted name must be accounted for.
    let explained: std::collections::HashSet<usize> = v
        .explained
        .iter()
        .chain(&v.schemas)
        .filter_map(tok_of)
        .collect();
    let punct = |k: usize, c: u8| {
        sig.get(k).is_some_and(|&t| {
            toks[t].kind == TokenKind::Punct && text.as_bytes()[toks[t].start as usize] == c
        })
    };
    for (n, &k) in sig.iter().enumerate() {
        if !toks[k].kind.is_ident_like() || !punct(n + 1, b'.') {
            continue;
        }
        if n > 0 && punct(n - 1, b'.') {
            continue; // not the first part
        }
        if !explained.contains(&k) {
            return None;
        }
    }
    Some((found, ddl_target(&stmt, current)))
}

/// A name from the AST: `name` or `db.name`.
fn ddl_name(n: &ObjectName, current: Option<&str>) -> Option<DdlName> {
    let ids = idents(n);
    if ids.len() != n.0.len() {
        return None;
    }
    match ids.as_slice() {
        [name] => Some(DdlName::unqualified(&name.value, current)),
        [db, name] => Some(DdlName::qualified(&db.value, &name.value)),
        _ => None,
    }
}

/// The DDL target of a modelled statement, as the AST gives it.
#[allow(clippy::too_many_lines)]
fn ddl_target(s: &Statement, current: Option<&str>) -> Option<DdlTarget> {
    let name = |n: &ObjectName| ddl_name(n, current);
    match s {
        Statement::CreateTable(ct) => {
            if ct.clone.is_some() {
                return None;
            }
            let like = match &ct.like {
                Some(CreateTableLikeKind::Parenthesized(l) | CreateTableLikeKind::Plain(l)) => {
                    Some(name(&l.name)?)
                }
                None => None,
            };
            Some(DdlTarget::CreateObject {
                kind: DdlKind::Table,
                name: name(&ct.name)?,
                if_not_exists: ct.if_not_exists,
                or_replace: ct.or_replace,
                temporary: ct.temporary,
                parent: None,
                like,
                as_select: ct.query.is_some(),
            })
        }
        Statement::CreateView(cv) => {
            if cv.materialized || cv.secure || cv.or_alter {
                return None;
            }
            Some(DdlTarget::CreateObject {
                kind: DdlKind::View,
                name: name(&cv.name)?,
                if_not_exists: cv.if_not_exists,
                or_replace: cv.or_replace,
                temporary: cv.temporary,
                parent: None,
                like: None,
                as_select: false,
            })
        }
        Statement::AlterTable(at) => {
            if at.only || at.table_type.is_some() {
                return None;
            }
            let table = name(&at.name)?;
            let mut rename_to = None;
            for op in &at.operations {
                if let AlterTableOperation::RenameTable { table_name } = op {
                    let (RenameTableNameKind::As(n) | RenameTableNameKind::To(n)) = table_name;
                    rename_to = Some(name(n)?);
                }
            }
            match (rename_to, at.operations.len()) {
                (Some(to), 1) => Some(DdlTarget::RenameTables {
                    pairs: vec![(table, to)],
                    if_exists: at.if_exists,
                }),
                (rename_to, _) => Some(DdlTarget::AlterObject {
                    kind: DdlKind::Table,
                    name: table,
                    if_exists: at.if_exists,
                    op: AlterOp::Change { rename_to },
                }),
            }
        }
        Statement::Truncate(t) => match t.table_names.as_slice() {
            [target] if !t.if_exists && t.partitions.is_none() && !target.only => {
                Some(DdlTarget::Truncate {
                    name: name(&target.name)?,
                })
            }
            _ => None,
        },
        Statement::CreateIndex(ci) => {
            let index = match &ci.name {
                Some(n) => Some(idents(n).last()?.value.clone()),
                None => None,
            };
            Some(DdlTarget::AlterObject {
                kind: DdlKind::Table,
                name: name(&ci.table_name)?,
                if_exists: false,
                op: AlterOp::CreateIndex {
                    name: index,
                    if_not_exists: ci.if_not_exists,
                    or_replace: false,
                },
            })
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            temporary,
            table: None,
            ..
        } => {
            let kind = match object_type {
                sqlparser::ast::ObjectType::Table => DdlKind::Table,
                sqlparser::ast::ObjectType::View => DdlKind::View,
                _ => return None,
            };
            Some(DdlTarget::DropObjects {
                kind,
                names: names.iter().map(name).collect::<Option<Vec<_>>>()?,
                if_exists: *if_exists,
                temporary: *temporary,
            })
        }
        _ => None,
    }
}

/// Deepest parenthesis and `CASE` nesting sent to sqlparser-rs. Its
/// recursive descent uses several kilobytes of stack per level in debug
/// builds and is built without `recursive-protection` (which would pull in
/// `stacker`), so deeper statements are left to the walker, whose own
/// depth is bounded.
const MAX_AST_NESTING: usize = 12;

/// Most significant tokens sent to sqlparser-rs. Its trees are as deep as
/// an operator chain is long (`a AND b AND ...`), and visiting or dropping
/// them recurses; longer statements are left to the walker, which is
/// linear. Typical OLTP statements are far shorter.
const MAX_AST_TOKENS: usize = 400;

/// sqlparser-rs's recursion limit for the AST path (its default is 50).
const PARSER_RECURSION_LIMIT: usize = 16;

/// The deepest nesting of `(` and `CASE` in the statement's tokens.
fn nesting(text: &[u8], toks: &[Token]) -> usize {
    let (mut depth, mut max) = (0usize, 0usize);
    for t in toks {
        let b = t.bytes(text);
        let open = (t.kind == TokenKind::Punct && b == b"(")
            || (t.kind == TokenKind::Word && b.eq_ignore_ascii_case(b"CASE"));
        let close = (t.kind == TokenKind::Punct && b == b")")
            || (t.kind == TokenKind::Word && b.eq_ignore_ascii_case(b"END"));
        if open {
            depth += 1;
            max = max.max(depth);
        } else if close {
            depth = depth.saturating_sub(1);
        }
    }
    max
}

/// The statement kinds the visitor knows every name position of.
fn modelled(s: &Statement) -> bool {
    match s {
        Statement::Query(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::AlterTable(_)
        | Statement::Truncate(_)
        | Statement::CreateIndex(_) => true,
        Statement::Drop { object_type, .. } => matches!(
            object_type,
            sqlparser::ast::ObjectType::Table | sqlparser::ast::ObjectType::View
        ),
        _ => false,
    }
}

/// The parse view of a statement's tokens; `None` for text the view can't
/// keep faithful.
pub(crate) fn parse_view(text: &str, toks: &[Token]) -> Option<String> {
    let bytes = text.as_bytes();
    let first = toks.first()?.start as usize;
    let last = toks.last()?.end as usize;
    let mut v = String::with_capacity(last - first);
    let blank = |v: &mut String, s: &str| {
        for ch in s.chars() {
            v.push(if ch == '\n' { '\n' } else { ' ' });
        }
    };
    let xs = |v: &mut String, s: &str| {
        for ch in s.chars() {
            v.push(if ch == '\n' { '\n' } else { 'x' });
        }
    };
    for (k, t) in toks.iter().enumerate() {
        let s = &text[t.range()];
        match t.kind {
            k2 if k2.is_trivia() => blank(&mut v, s),
            TokenKind::String => {
                v.push('\'');
                xs(&mut v, &s[1..s.len() - 1]);
                v.push('\'');
            }
            TokenKind::NationalString => {
                v.push_str(&s[..1]);
                v.push('\'');
                xs(&mut v, &s[2..s.len() - 1]);
                v.push('\'');
            }
            TokenKind::QuotedIdent => {
                if s.starts_with('"') {
                    let inner = &s[1..s.len() - 1];
                    if inner.contains('`') {
                        return None;
                    }
                    v.push('`');
                    v.push_str(inner);
                    v.push('`');
                } else {
                    v.push_str(s);
                }
            }
            TokenKind::Word if bytes[t.start as usize].is_ascii_digit() => return None,
            TokenKind::UserVar if s.contains(['\'', '"', '`', '.']) => return None,
            TokenKind::SysVar if s.contains('`') => return None,
            TokenKind::Op
                if s == "-"
                    && toks.get(k + 1).is_some_and(|n| {
                        n.start == t.end && n.kind == TokenKind::Op && &text[n.range()] == "-"
                    }) =>
            {
                return None;
            }
            TokenKind::Unterminated | TokenKind::Other => return None,
            _ => v.push_str(s),
        }
    }
    Some(v)
}

/// What the visitor saw, as identifier start positions.
#[derive(Default)]
struct Collect {
    /// Database parts.
    schemas: Vec<(u64, u64)>,
    /// First parts of dotted names that are not databases (`t.c`, `t.*`).
    explained: Vec<(u64, u64)>,
}

fn pos(i: &Ident) -> (u64, u64) {
    (i.span.start.line, i.span.start.column)
}

fn idents(n: &ObjectName) -> Vec<&Ident> {
    n.0.iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

impl Collect {
    /// An object name in an object position: `db.obj` names a database.
    fn object(&mut self, n: &ObjectName) {
        let ids = idents(n);
        match ids.len() {
            0 | 1 => {}
            _ => self.schemas.push(pos(ids[0])),
        }
    }

    /// A qualified wildcard: `t.*` or `db.t.*`.
    fn wildcard(&mut self, n: &ObjectName) {
        let ids = idents(n);
        match ids.len() {
            0 => {}
            1 => self.explained.push(pos(ids[0])),
            _ => self.schemas.push(pos(ids[0])),
        }
    }

    /// A column reference: `t.c` or `db.t.c`.
    fn column(&mut self, ids: &[&Ident]) {
        match ids.len() {
            0 | 1 => {}
            2 => self.explained.push(pos(ids[0])),
            _ => self.schemas.push(pos(ids[0])),
        }
    }

    fn assignment_target(&mut self, t: &AssignmentTarget) {
        let names: Vec<&ObjectName> = match t {
            AssignmentTarget::ColumnName(n) => vec![n],
            AssignmentTarget::Tuple(ns) => ns.iter().collect(),
        };
        for n in names {
            self.column(&idents(n));
        }
    }

    fn constraint(&mut self, c: &TableConstraint) {
        if let TableConstraint::ForeignKey(fk) = c {
            self.object(&fk.foreign_table);
        }
    }

    /// The name positions a generic visit does not reach.
    fn statement(&mut self, s: &Statement) {
        match s {
            Statement::Insert(i) => {
                if let Some(OnInsert::DuplicateKeyUpdate(a)) = &i.on {
                    for x in a {
                        self.assignment_target(&x.target);
                    }
                }
                for c in &i.columns {
                    self.column(&idents(c));
                }
            }
            Statement::Update(u) => {
                for x in &u.assignments {
                    self.assignment_target(&x.target);
                }
            }
            Statement::Delete(d) => {
                for n in &d.tables {
                    self.wildcard_or_object(n);
                }
            }
            Statement::CreateTable(ct) => {
                match &ct.like {
                    Some(CreateTableLikeKind::Parenthesized(l) | CreateTableLikeKind::Plain(l)) => {
                        self.object(&l.name);
                    }
                    None => {}
                }
                if let Some(c) = &ct.clone {
                    self.object(c);
                }
                for c in &ct.constraints {
                    self.constraint(c);
                }
                for col in &ct.columns {
                    for o in &col.options {
                        if let ColumnOption::ForeignKey(fk) = &o.option {
                            self.object(&fk.foreign_table);
                        }
                    }
                }
            }
            Statement::AlterTable(at) => {
                for op in &at.operations {
                    match op {
                        AlterTableOperation::RenameTable { table_name } => match table_name {
                            RenameTableNameKind::As(n) | RenameTableNameKind::To(n) => {
                                self.object(n);
                            }
                        },
                        AlterTableOperation::AddConstraint { constraint, .. } => {
                            self.constraint(constraint);
                        }
                        _ => {}
                    }
                }
            }
            Statement::Drop { names, table, .. } => {
                for n in names {
                    self.object(n);
                }
                if let Some(t) = table {
                    self.object(t);
                }
            }
            _ => {}
        }
    }

    /// MariaDB's `NEXTVAL(db.seq)`, `LASTVAL(db.seq)` and `SETVAL(db.seq,
    /// ...)` take a sequence name as their first argument, which the parser
    /// reads as a column.
    fn sequence_argument(&mut self, f: &Function) {
        let ids = idents(&f.name);
        let is_seq_fn = ids.len() == 1
            && ["NEXTVAL", "LASTVAL", "SETVAL"]
                .iter()
                .any(|n| ids[0].value.eq_ignore_ascii_case(n));
        if !is_seq_fn {
            return;
        }
        if let FunctionArguments::List(list) = &f.args
            && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(
                parts,
            )))) = list.args.first()
            && parts.len() == 2
        {
            self.schemas.push(pos(&parts[0]));
        }
    }

    /// A multi-table `DELETE` target: `t`, `db.t`, `t.*` or `db.t.*`.
    fn wildcard_or_object(&mut self, n: &ObjectName) {
        self.object(n);
    }
}

impl Visitor for Collect {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        self.object(relation);
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        for item in &select.projection {
            if let SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(n),
                _,
            ) = item
            {
                self.wildcard(n);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::CompoundIdentifier(ids) => {
                let ids: Vec<&Ident> = ids.iter().collect();
                self.column(&ids);
            }
            Expr::Function(f) => {
                self.object(&f.name);
                self.sequence_argument(f);
            }
            Expr::QualifiedWildcard(n, _) => self.wildcard(n),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
