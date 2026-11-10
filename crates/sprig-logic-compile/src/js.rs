//! The JavaScript front end (chapter 41, "JavaScript front end", "The
//! JavaScript subset"; build step 5).
//!
//! `oxc_parser` reads the source; this module lowers the compilable subset
//! into the shared IR, which the checks and the PL/SQL generator then
//! treat exactly as KIRun's. Anything outside the subset is a placement
//! reason (`js.*`), never an error: the version then runs in the engine
//! tier on QuickJS (`sprig-logic-js::js`), which defines the language.
//!
//! The two forms of a body are QuickJS's: a module whose default export
//! is the function (`import { db, sprig } from 'sprig:db'` is the only
//! import), or the function's body alone with the parameters in scope by
//! name (MySQL 9). Parameter types come from the declaration.
//!
//! What compiles, and how it keeps JavaScript's meaning:
//!
//! - `const`, `let`, `var` with an initializer; `if`/`else`, `while`,
//!   `for (init; test; update)`, `for (const x of rows)`, `break`,
//!   `return`, `throw`, blocks;
//! - numbers, strings, booleans, `null` and `undefined` (both `NULL`),
//!   template literals, object and array literals;
//! - arithmetic, where a possibly-`null` number reads as 0 as in
//!   JavaScript (`COALESCE(x, 0)`); `+` with text concatenates (`null`
//!   reads as `"null"`); `/` is always a double; `**`;
//! - `=== !==` (operands of different kinds are never equal), `== !=`
//!   against `null` or between values of one kind, `< <= > >=` on numbers
//!   (`null` as 0) or on two non-null strings (by code point);
//! - `&& || ??` and `?:`, with JavaScript's truthiness (`''`, `0` and
//!   `null` are false);
//! - `db.sql`, `db.sqlOne`, `db.exec` with a literal SQL text and an
//!   array literal of arguments, as a declaration's initializer or a
//!   statement; a `sqlOne` row's columns, an `exec`'s `affected`; the
//!   columns of each row of `for (const r of db.sql(...))`;
//! - `Math.abs floor ceil round sqrt pow max min`; `length`,
//!   `toUpperCase`, `toLowerCase`, `includes`, `startsWith`, `endsWith`,
//!   `indexOf` on non-null text; `String(x)`;
//! - `return {fields}`, `return sprig.event(name, {fields})`,
//!   `sprig.reject(message)`, `throw new Error(message)`.
//!
//! Reading a column of a `sqlOne` row that may be `null` throws a
//! `TypeError` in JavaScript, so the compiled code checks the row first
//! and fails with 39865 (a guard before the statement) unless the code has
//! already tested it (`if (p === null) return ...`, `p !== null && ...`).
//! A read that is conditional on something else is a reason.
//!
//! Known differences (listed by `EXPLAIN FUNCTION`, chapter 41's table):
//! whole numbers past 2^63 overflow (39866) where JavaScript loses
//! precision; `length` and `indexOf` count code points; number-to-text is
//! exact for integers and up to 15 significant digits; `ctx.log` and
//! `console.log` lines are dropped.

use std::collections::{BTreeMap, BTreeSet};

use oxc_allocator::Allocator;
use oxc_ast::ast::{
    Argument, AssignmentTarget, BindingPattern, ExportDefaultDeclarationKind, Expression,
    ForStatementInit, ForStatementLeft, FormalParameters, ImportDeclarationSpecifier,
    ObjectPropertyKind, Program, SimpleAssignmentTarget, Statement, VariableDeclaration,
};
use oxc_parser::{ParseOptions, Parser};
use oxc_span::{GetSpan, SourceType, Span};
use oxc_syntax::operator::{
    AssignmentOperator, BinaryOperator, LogicalOperator, UnaryOperator, UpdateOperator,
};
use sprig_common::Result;

use crate::check::Analysis;
use crate::ir::{
    BinOp, Block, Bound, Builtin, DbOp, ErrorSpec, EventDef, Expr, Function, Literal, LocalId,
    Param, PathSeg, SqlShape, Stmt, Ty, UnOp,
};
use crate::sql::{self, Columns, Want};
use crate::{Compiled, Options, Reason};

/// A JavaScript function's signature, from its declaration.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Signature {
    /// Parameters and their types, in call order.
    pub params: Vec<(String, Ty)>,
    /// `RETURNS t`: the result is `output {value}`.
    pub returns: Option<Ty>,
}

/// JavaScript without its declaration's signature: always the engine
/// tier (callers with a declaration use [`compile_with`]).
#[must_use]
pub fn compile(_source: &str) -> Compiled {
    refused(Reason::new(
        "js.signature",
        "source",
        "JavaScript compiles only with its declaration's parameter types",
    ))
}

fn refused(r: Reason) -> Compiled {
    Compiled {
        ir: None,
        analysis: Analysis::refused(r),
        plsql: None,
    }
}

/// Compiles `source`, a JavaScript function `name` with signature `sig`,
/// against the shard schema's columns `cat`. A source outside the subset
/// gives reasons (the engine tier), not an error.
///
/// # Errors
/// 39874 when embedded SQL and its arguments disagree.
pub fn compile_with(
    name: &str,
    source: &str,
    sig: &Signature,
    cat: &dyn Columns,
    opts: &Options,
) -> Result<Compiled> {
    // Locals whose declared type later assignments widen: the lowering
    // runs again with the wider type (numbers are all doubles in
    // JavaScript, so `let t = 0; t += 1.5` is one variable).
    let mut widen: BTreeMap<u32, Ty> = BTreeMap::new();
    for _ in 0..6 {
        let alloc = Allocator::default();
        let parsed = Parser::new(&alloc, source, SourceType::mjs())
            .with_options(ParseOptions {
                allow_return_outside_function: true,
                ..ParseOptions::default()
            })
            .parse();
        if let Some(e) = parsed.errors.first() {
            return Ok(refused(Reason::new(
                "js.parse",
                "source",
                format!("the JavaScript parser refused it: {e}"),
            )));
        }
        let mut l = L::new(name, source, sig, cat, widen.clone());
        l.program(&parsed.program);
        if let Some(e) = l.fatal.take() {
            return Err(e);
        }
        if l.widen.len() > widen.len() && l.widen != widen {
            widen = l.widen;
            continue;
        }
        let (f, reasons, lints) = l.finish();
        return Ok(crate::finish(f, reasons, lints, opts));
    }
    Ok(refused(Reason::new(
        "js.types",
        "source",
        "the variables' types don't settle",
    )))
}

/// What a name stands for.
#[derive(Debug, Clone)]
enum Var {
    Param(u16),
    Local(LocalId),
    /// A `db.sqlOne` result: `found`, and each column's local.
    Row {
        found: LocalId,
        cols: Vec<(String, LocalId)>,
    },
    /// A `db.exec` result.
    Exec {
        affected: LocalId,
    },
    /// A `db.sql` result (a JSON array of rows) and its columns.
    Rows {
        local: LocalId,
        cols: Vec<(String, Ty)>,
    },
    /// One row of [`Var::Rows`] in `for ... of`.
    Elem {
        local: LocalId,
        cols: Vec<(String, Ty)>,
    },
    /// `db` or `sprig` from `sprig:db` (or the globals).
    Module(&'static str),
}

/// A fact key: a parameter or a local (a row's `found`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    P(u16),
    L(LocalId),
}

/// A lowered value: its IR, its type, and whether it is never `NULL`.
#[derive(Debug, Clone)]
struct V {
    e: Expr,
    ty: Ty,
    nn: bool,
}

impl V {
    fn lit(l: Literal) -> Self {
        let nn = l != Literal::Null;
        V {
            ty: l.ty(),
            e: Expr::Lit(l),
            nn,
        }
    }
    fn null() -> Self {
        V::lit(Literal::Null)
    }
}

/// A failed lowering: the reason is recorded; the caller gives up on the
/// statement.
struct Stop;

type R<T> = std::result::Result<T, Stop>;

struct L<'s, 'c> {
    src: &'s str,
    sig: &'s Signature,
    cat: &'c dyn Columns,
    f: Function,
    reasons: Vec<Reason>,
    lints: Vec<Reason>,
    scopes: Vec<Vec<(String, Var)>>,
    facts: BTreeSet<Key>,
    /// Unconditional reads of rows that may be `null`, for the guard
    /// before the statement.
    guards: Vec<(LocalId, String)>,
    /// Inside a conditionally evaluated operand.
    cond: u32,
    loops: u32,
    /// Declaration offset → wider type for the next pass.
    widen: BTreeMap<u32, Ty>,
    /// Event name → fields in first-seen order.
    events: Vec<(String, Vec<(String, Ty)>)>,
    tmp: u32,
    /// Each local's declaration offset (for widening).
    decls: BTreeMap<LocalId, u32>,
    /// An error that isn't a reason (39874).
    fatal: Option<sprig_common::SprigError>,
}

/// Whether control can't continue after the lowered statements.
fn exits(b: &Block) -> bool {
    match b.last() {
        Some(Stmt::Return | Stmt::Throw(_)) => true,
        Some(Stmt::If(_, a, c)) => exits(a) && exits(c),
        _ => false,
    }
}

impl<'s, 'c> L<'s, 'c> {
    fn new(
        name: &str,
        src: &'s str,
        sig: &'s Signature,
        cat: &'c dyn Columns,
        widen: BTreeMap<u32, Ty>,
    ) -> Self {
        let mut f = Function {
            name: name.to_owned(),
            ..Function::default()
        };
        f.params = sig
            .params
            .iter()
            .map(|(n, t)| Param {
                name: n.clone(),
                ty: *t,
                min: None,
                max: None,
            })
            .collect();
        L {
            src,
            sig,
            cat,
            f,
            reasons: Vec::new(),
            lints: Vec::new(),
            scopes: vec![Vec::new()],
            facts: BTreeSet::new(),
            guards: Vec::new(),
            cond: 0,
            loops: 0,
            widen,
            events: vec![("output".to_owned(), Vec::new())],
            tmp: 0,
            decls: BTreeMap::new(),
            fatal: None,
        }
    }

    fn at(&self, span: Span) -> String {
        let start = (span.start as usize).min(self.src.len());
        let before = &self.src[..start];
        let line = before.matches('\n').count() + 1;
        let col = start - before.rfind('\n').map_or(0, |i| i + 1) + 1;
        format!("line {line}, column {col}")
    }

    fn stop<T>(&mut self, code: &str, span: Span, detail: impl Into<String>) -> R<T> {
        let at = self.at(span);
        self.reasons.push(Reason::new(code, &at, detail));
        Err(Stop)
    }

    fn finish(mut self) -> (Function, Vec<Reason>, Vec<Reason>) {
        if self.sig.returns.is_some() {
            let ty = self.sig.returns.unwrap_or(Ty::Json);
            self.events[0].1 = vec![("value".to_owned(), ty)];
        }
        self.f.events = self
            .events
            .into_iter()
            .map(|(name, fields)| EventDef { name, fields })
            .collect();
        (self.f, self.reasons, self.lints)
    }

    // -- names --------------------------------------------------------------

    fn bind(&mut self, name: &str, v: Var) {
        if let Some(s) = self.scopes.last_mut() {
            s.push((name.to_owned(), v));
        }
    }

    fn lookup(&self, name: &str) -> Option<Var> {
        self.scopes
            .iter()
            .rev()
            .flat_map(|s| s.iter().rev())
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    }

    fn key(v: &Var) -> Option<Key> {
        match v {
            Var::Param(i) => Some(Key::P(*i)),
            Var::Local(l) => Some(Key::L(*l)),
            Var::Row { found, .. } => Some(Key::L(*found)),
            _ => None,
        }
    }

    fn scoped<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.scopes.push(Vec::new());
        let r = f(self);
        self.scopes.pop();
        r
    }

    // -- the program ------------------------------------------------------------

    fn program(&mut self, p: &Program<'_>) {
        let module = p.body.iter().any(|s| {
            matches!(
                s,
                Statement::ImportDeclaration(_)
                    | Statement::ExportDefaultDeclaration(_)
                    | Statement::ExportNamedDeclaration(_)
                    | Statement::ExportAllDeclaration(_)
            )
        });
        if !module {
            for (i, (n, _)) in self.sig.params.iter().enumerate() {
                let n = n.clone();
                self.bind(&n, Var::Param(u16::try_from(i).unwrap_or(u16::MAX)));
            }
            self.bind("db", Var::Module("db"));
            self.bind("sprig", Var::Module("sprig"));
            let body = self.block(&p.body);
            self.f.body = self.close(body);
            return;
        }
        let mut entry = None;
        for s in &p.body {
            match s {
                Statement::ImportDeclaration(d) => {
                    if d.source.value.as_str() != "sprig:db" {
                        let _ =
                            self.stop::<()>("js.import", d.span, "only sprig:db can be imported");
                        continue;
                    }
                    for sp in d.specifiers.iter().flatten() {
                        match sp {
                            ImportDeclarationSpecifier::ImportSpecifier(x)
                                if matches!(x.imported.name().as_str(), "db" | "sprig") =>
                            {
                                let m = if x.imported.name().as_str() == "db" {
                                    "db"
                                } else {
                                    "sprig"
                                };
                                self.bind(x.local.name.as_str(), Var::Module(m));
                            }
                            _ => {
                                let _ = self.stop::<()>(
                                    "js.import",
                                    d.span,
                                    "sprig:db exports db and sprig",
                                );
                            }
                        }
                    }
                }
                Statement::ExportDefaultDeclaration(d) => match &d.declaration {
                    ExportDefaultDeclarationKind::FunctionDeclaration(func)
                    | ExportDefaultDeclarationKind::FunctionExpression(func) => {
                        if func.r#async || func.generator {
                            let _ = self.stop::<()>(
                                "js.async",
                                d.span,
                                "async functions and generators run in the engine tier",
                            );
                        } else if let Some(b) = &func.body {
                            entry = Some((&*func.params, &b.statements, d.span));
                        }
                    }
                    ExportDefaultDeclarationKind::ArrowFunctionExpression(a)
                        if !a.r#async && !a.expression =>
                    {
                        entry = Some((&*a.params, &a.body.statements, d.span));
                    }
                    _ => {
                        let _ = self.stop::<()>(
                            "js.export",
                            d.span,
                            "the default export must be a function",
                        );
                    }
                },
                other => {
                    let _ = self.stop::<()>(
                        "js.module_scope",
                        other.span(),
                        "only imports and the default export compile at module level",
                    );
                }
            }
        }
        let Some((params, body, span)) = entry else {
            if self.reasons.is_empty() {
                let _ = self.stop::<()>("js.export", p.span, "no default export function");
            }
            return;
        };
        if self.params(params, span).is_err() {
            return;
        }
        let body = self.scoped(|l| l.block(body));
        self.f.body = self.close(body);
    }

    fn params(&mut self, ps: &FormalParameters<'_>, span: Span) -> R<()> {
        if ps.rest.is_some() || ps.items.len() != self.sig.params.len() {
            return self.stop(
                "js.params",
                span,
                format!(
                    "the function takes {} parameters; the declaration lists {}",
                    ps.items.len(),
                    self.sig.params.len()
                ),
            );
        }
        for (i, p) in ps.items.iter().enumerate() {
            match &p.pattern {
                BindingPattern::BindingIdentifier(b) if p.initializer.is_none() => {
                    self.bind(
                        b.name.as_str(),
                        Var::Param(u16::try_from(i).unwrap_or(u16::MAX)),
                    );
                }
                _ => return self.stop("js.params", p.span, "parameters must be plain names"),
            }
        }
        Ok(())
    }

    /// Ends the body: falling off the end is `output {}` (or
    /// `output {value: null}` for `RETURNS t`).
    fn close(&mut self, mut body: Block) -> Block {
        if !exits(&body) {
            let fields = if self.sig.returns.is_some() {
                vec![("value".to_owned(), Expr::Lit(Literal::Null))]
            } else {
                Vec::new()
            };
            body.push(Stmt::Emit("output".to_owned(), fields));
            body.push(Stmt::Return);
        }
        body
    }

    // -- statements ----------------------------------------------------------------

    fn block(&mut self, stmts: &[Statement<'_>]) -> Block {
        let mut out = Vec::new();
        for s in stmts {
            if exits(&out) {
                break;
            }
            let mut piece = Vec::new();
            let r = self.stmt(s, &mut piece);
            let guards = std::mem::take(&mut self.guards);
            if r.is_err() {
                continue;
            }
            out.extend(self.guard_stmts(guards));
            out.extend(piece);
        }
        out
    }

    /// The checks for the rows a statement reads unconditionally; after
    /// them the rows are known to be there.
    fn guard_stmts(&mut self, guards: Vec<(LocalId, String)>) -> Block {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for (found, col) in guards {
            if !seen.insert(found) {
                continue;
            }
            out.push(Stmt::If(
                Expr::Unary(UnOp::Not, Box::new(Expr::Local(found))),
                vec![Stmt::Throw(ErrorSpec {
                    code: 39865,
                    message: Expr::Lit(Literal::Str(format!(
                        "TypeError: cannot read properties of null (reading '{col}')"
                    ))),
                })],
                Vec::new(),
            ));
            self.facts.insert(Key::L(found));
        }
        out
    }

    fn sub_block(&mut self, s: &Statement<'_>) -> Block {
        self.scoped(|l| match s {
            Statement::BlockStatement(b) => l.block(&b.body),
            other => l.block(std::slice::from_ref(other)),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn stmt(&mut self, s: &Statement<'_>, out: &mut Block) -> R<()> {
        match s {
            Statement::EmptyStatement(_) => Ok(()),
            Statement::BlockStatement(b) => {
                let inner = self.scoped(|l| l.block(&b.body));
                out.extend(inner);
                Ok(())
            }
            Statement::VariableDeclaration(d) => self.declare(d, out),
            Statement::ExpressionStatement(e) => self.expr_stmt(&e.expression, out),
            Statement::IfStatement(i) => {
                let test = self.test(&i.test)?;
                let before = self.facts.clone();
                let guards = std::mem::take(&mut self.guards);
                out.extend(self.guard_stmts(guards));
                let base = self.facts.clone();
                let (t_true, t_false) =
                    (self.facts_if(&i.test, true), self.facts_if(&i.test, false));
                self.facts.extend(t_true);
                let a = self.sub_block(&i.consequent);
                let after_a = std::mem::replace(&mut self.facts, base.clone());
                self.facts.extend(t_false);
                let b = match &i.alternate {
                    Some(alt) => self.sub_block(alt),
                    None => Vec::new(),
                };
                let after_b = std::mem::take(&mut self.facts);
                self.facts = match (exits(&a), exits(&b)) {
                    (true, true) => before,
                    (true, false) => after_b,
                    (false, true) => after_a,
                    (false, false) => after_a.intersection(&after_b).copied().collect(),
                };
                out.push(Stmt::If(test, a, b));
                Ok(())
            }
            Statement::WhileStatement(w) => {
                self.loop_entry();
                let test = self.loop_test(&w.test)?;
                self.loops += 1;
                let body = self.sub_block(&w.body);
                self.loops -= 1;
                self.loop_entry();
                out.push(Stmt::While {
                    cond: test,
                    body,
                    bound: Bound::Guarded,
                });
                Ok(())
            }
            Statement::ForStatement(fs) => {
                self.scopes.push(Vec::new());
                let r = (|| {
                    let mut static_from = None;
                    match &fs.init {
                        Some(ForStatementInit::VariableDeclaration(d)) => {
                            if let [one] = d.declarations.as_slice()
                                && let Some(Expression::NumericLiteral(n)) = &one.init
                            {
                                static_from = Some(n.value);
                            }
                            self.declare(d, out)?;
                        }
                        Some(other) => {
                            if let Some(e) = other.as_expression() {
                                self.expr_stmt(e, out)?;
                            }
                        }
                        None => {}
                    }
                    let guards = std::mem::take(&mut self.guards);
                    out.extend(self.guard_stmts(guards));
                    self.loop_entry();
                    let test = match &fs.test {
                        Some(t) => self.loop_test(t)?,
                        None => Expr::bool(true),
                    };
                    let bound = match (static_from, &fs.test, &fs.update) {
                        (
                            Some(from),
                            Some(Expression::BinaryExpression(b)),
                            Some(Expression::UpdateExpression(u)),
                        ) if u.operator == UpdateOperator::Increment
                            && matches!(b.operator, BinaryOperator::LessThan)
                            && from.fract() == 0.0 =>
                        {
                            match &b.right {
                                Expression::NumericLiteral(to) if to.value.fract() == 0.0 => {
                                    Bound::Static((to.value - from).max(0.0) as u64)
                                }
                                _ => Bound::Guarded,
                            }
                        }
                        _ => Bound::Guarded,
                    };
                    self.loops += 1;
                    let mut body = self.sub_block(&fs.body);
                    self.loops -= 1;
                    if let Some(u) = &fs.update {
                        if exits(&body) {
                            // The update never runs; nothing to add.
                        } else {
                            let mut upd = Vec::new();
                            self.expr_stmt(u, &mut upd)?;
                            body.extend(upd);
                        }
                    }
                    self.loop_entry();
                    out.push(Stmt::While {
                        cond: test,
                        body,
                        bound,
                    });
                    Ok(())
                })();
                self.scopes.pop();
                r
            }
            Statement::ForOfStatement(fo) => self.for_of(fo, out),
            Statement::BreakStatement(b) => {
                if b.label.is_some() || self.loops == 0 {
                    return self.stop("js.label", b.span, "labelled break");
                }
                out.push(Stmt::Break);
                Ok(())
            }
            Statement::ReturnStatement(r) => self.ret(r.argument.as_ref(), r.span, out),
            Statement::ThrowStatement(t) => {
                let msg = match &t.argument {
                    Expression::NewExpression(n) => match (&n.callee, n.arguments.first()) {
                        (Expression::Identifier(id), a) if id.name.as_str().ends_with("Error") => {
                            match a.and_then(Argument::as_expression) {
                                Some(m) => {
                                    let v = self.val(m)?;
                                    self.text_of(v, m.span())?
                                }
                                None => Expr::Lit(Literal::Str(String::new())),
                            }
                        }
                        _ => return self.stop("js.throw", t.span, "throw new Error(message)"),
                    },
                    other => {
                        let v = self.val(other)?;
                        self.text_of(v, other.span())?
                    }
                };
                out.push(Stmt::Throw(ErrorSpec {
                    code: 39865,
                    message: msg,
                }));
                Ok(())
            }
            Statement::ContinueStatement(c) => {
                self.stop("js.continue", c.span, "continue runs in the engine tier")
            }
            Statement::TryStatement(t) => {
                self.stop("js.try", t.span, "try/catch runs in the engine tier")
            }
            Statement::SwitchStatement(t) => {
                self.stop("js.switch", t.span, "switch runs in the engine tier")
            }
            Statement::DoWhileStatement(t) => {
                self.stop("js.do_while", t.span, "do/while runs in the engine tier")
            }
            Statement::FunctionDeclaration(t) => self.stop(
                "js.helper",
                t.span,
                "helper functions aren't inlined yet; they run in the engine tier",
            ),
            other => self.stop(
                "js.statement",
                other.span(),
                "this statement runs in the engine tier",
            ),
        }
    }

    /// A loop's entry: facts about locals may not hold on every
    /// iteration.
    fn loop_entry(&mut self) {
        let rows: BTreeSet<LocalId> = self
            .scopes
            .iter()
            .flatten()
            .filter_map(|(_, v)| match v {
                Var::Row { found, .. } => Some(*found),
                _ => None,
            })
            .collect();
        self.facts.retain(|k| match k {
            Key::P(_) => true,
            Key::L(l) => rows.contains(l),
        });
    }

    fn loop_test(&mut self, e: &Expression<'_>) -> R<Expr> {
        let t = self.test(e)?;
        if !self.guards.is_empty() {
            self.guards.clear();
            return self.stop(
                "js.null_row",
                e.span(),
                "a loop test reads a row that may be null",
            );
        }
        Ok(t)
    }

    fn declare(&mut self, d: &VariableDeclaration<'_>, out: &mut Block) -> R<()> {
        for decl in &d.declarations {
            let BindingPattern::BindingIdentifier(id) = &decl.id else {
                return self.stop("js.destructuring", decl.span, "destructuring");
            };
            let name = id.name.as_str();
            let Some(init) = &decl.init else {
                return self.stop(
                    "js.untyped_local",
                    decl.span,
                    format!("give {name} a value where it is declared"),
                );
            };
            if let Some(v) = self.db_call(init, name, out)? {
                self.bind(name, v);
                continue;
            }
            let v = self.val(init)?;
            let key = decl.span.start;
            let ty = match (self.widen.get(&key), v.ty) {
                (Some(w), _) => *w,
                (None, Ty::Null) => {
                    return self.stop(
                        "js.untyped_local",
                        decl.span,
                        format!("{name} starts as null: give it a typed value"),
                    );
                }
                (None, t) => t,
            };
            let l = self.f.local(name, ty);
            let e = self.coerce(v.clone(), ty, decl.span)?;
            out.push(Stmt::Assign(l, e));
            self.facts.remove(&Key::L(l));
            if v.nn {
                self.facts.insert(Key::L(l));
            }
            self.bind(name, Var::Local(l));
            // Remember where it was declared, for widening.
            self.decls.insert(l, key);
        }
        Ok(())
    }

    fn expr_stmt(&mut self, e: &Expression<'_>, out: &mut Block) -> R<()> {
        match e {
            Expression::ParenthesizedExpression(p) => self.expr_stmt(&p.expression, out),
            Expression::CallExpression(c) => {
                if self.db_call(e, "", out)?.is_some() {
                    return Ok(());
                }
                match self.callee_path(&c.callee).as_deref() {
                    Some(["sprig", "reject"]) => {
                        let m = match c.arguments.first().and_then(Argument::as_expression) {
                            Some(a) => {
                                let v = self.val(a)?;
                                self.text_of(v, a.span())?
                            }
                            None => Expr::Lit(Literal::Str(String::new())),
                        };
                        out.push(Stmt::Throw(ErrorSpec {
                            code: 39863,
                            message: m,
                        }));
                        Ok(())
                    }
                    Some(["ctx" | "console", "log"]) => {
                        let at = self.at(c.span);
                        self.lints.push(Reason::new(
                            "js.log",
                            &at,
                            "log lines are dropped in the compiled tier",
                        ));
                        Ok(())
                    }
                    _ => self.stop("js.call", c.span, "this call runs in the engine tier"),
                }
            }
            Expression::AssignmentExpression(a) => {
                let AssignmentTarget::AssignmentTargetIdentifier(id) = &a.left else {
                    return self.stop("js.assign", a.span, "only variables can be assigned");
                };
                let rhs = self.val(&a.right)?;
                let op = match a.operator {
                    AssignmentOperator::Assign => None,
                    AssignmentOperator::Addition => Some(BinaryOperator::Addition),
                    AssignmentOperator::Subtraction => Some(BinaryOperator::Subtraction),
                    AssignmentOperator::Multiplication => Some(BinaryOperator::Multiplication),
                    AssignmentOperator::Division => Some(BinaryOperator::Division),
                    AssignmentOperator::Remainder => Some(BinaryOperator::Remainder),
                    _ => return self.stop("js.assign", a.span, "this assignment operator"),
                };
                self.assign(id.name.as_str(), op, rhs, a.span, out)
            }
            Expression::UpdateExpression(u) => {
                let SimpleAssignmentTarget::AssignmentTargetIdentifier(id) = &u.argument else {
                    return self.stop("js.assign", u.span, "only variables can be updated");
                };
                let op = if u.operator == UpdateOperator::Increment {
                    BinaryOperator::Addition
                } else {
                    BinaryOperator::Subtraction
                };
                self.assign(
                    id.name.as_str(),
                    Some(op),
                    V::lit(Literal::Int(1)),
                    u.span,
                    out,
                )
            }
            other => {
                // Evaluated for nothing: only plain values compile.
                self.val(other).map(|_| ())
            }
        }
    }

    fn assign(
        &mut self,
        name: &str,
        op: Option<BinaryOperator>,
        rhs: V,
        span: Span,
        out: &mut Block,
    ) -> R<()> {
        let Some(Var::Local(l)) = self.lookup(name) else {
            return self.stop(
                "js.assign",
                span,
                format!("{name} can't be assigned in the compiled tier"),
            );
        };
        let cur = V {
            e: Expr::Local(l),
            ty: self.f.local_def(l).ty,
            nn: self.facts.contains(&Key::L(l)),
        };
        let v = match op {
            None => rhs,
            Some(op) => self.binary(op, cur.clone(), rhs, span)?,
        };
        let ty = self.f.local_def(l).ty;
        let e = match self.coerce(v.clone(), ty, span) {
            Ok(e) => e,
            Err(Stop) => {
                // A wider type: run the lowering again with it.
                if let (Some(w), Some(key)) = (ty.join(v.ty), self.decls.get(&l).copied())
                    && w != ty
                {
                    self.reasons.pop();
                    self.widen.insert(key, w);
                }
                return Err(Stop);
            }
        };
        out.push(Stmt::Assign(l, e));
        self.facts.remove(&Key::L(l));
        if v.nn {
            self.facts.insert(Key::L(l));
        }
        Ok(())
    }

    fn for_of(&mut self, fo: &oxc_ast::ast::ForOfStatement<'_>, out: &mut Block) -> R<()> {
        if fo.r#await {
            return self.stop("js.async", fo.span, "for await");
        }
        let ForStatementLeft::VariableDeclaration(d) = &fo.left else {
            return self.stop("js.for_of", fo.span, "for (const x of ...)");
        };
        let [decl] = d.declarations.as_slice() else {
            return self.stop("js.for_of", fo.span, "one variable");
        };
        let BindingPattern::BindingIdentifier(id) = &decl.id else {
            return self.stop("js.destructuring", decl.span, "destructuring");
        };
        let (src, cols) = match &fo.right {
            Expression::Identifier(i) => match self.lookup(i.name.as_str()) {
                Some(Var::Rows { local, cols }) => (Expr::Local(local), Some(cols)),
                Some(Var::Local(l)) if self.f.local_def(l).ty == Ty::Json => (Expr::Local(l), None),
                _ => return self.stop("js.for_of", fo.span, "for ... of rows from db.sql"),
            },
            other => {
                let mut pre = Vec::new();
                match self.db_call(other, "rows", &mut pre)? {
                    Some(Var::Rows { local, cols }) => {
                        out.extend(pre);
                        (Expr::Local(local), Some(cols))
                    }
                    _ => return self.stop("js.for_of", fo.span, "for ... of rows from db.sql"),
                }
            }
        };
        self.tmp += 1;
        let n = self.tmp;
        let arr = self.f.local(&format!("of{n}_source"), Ty::Json);
        let len = self.f.local(&format!("of{n}_length"), Ty::Int);
        let cur = self.f.local(&format!("of{n}_index"), Ty::Int);
        out.push(Stmt::Assign(arr, src));
        out.push(Stmt::Assign(
            len,
            Expr::bin(
                BinOp::Coalesce,
                Expr::call(Builtin::JsonLength, vec![Expr::Local(arr)]),
                Expr::int(0),
            ),
        ));
        out.push(Stmt::Assign(cur, Expr::int(0)));
        let each = self.f.local(id.name.as_str(), Ty::Json);
        self.loop_entry();
        let body = self.scoped(|l| {
            l.bind(
                id.name.as_str(),
                match cols {
                    Some(cols) => Var::Elem { local: each, cols },
                    None => Var::Local(each),
                },
            );
            l.loops += 1;
            let b = l.sub_block(&fo.body);
            l.loops -= 1;
            b
        });
        self.loop_entry();
        let mut full = vec![
            Stmt::Assign(
                each,
                Expr::Path(
                    Box::new(Expr::Local(arr)),
                    vec![PathSeg::Index(Box::new(Expr::Local(cur)))],
                ),
            ),
            Stmt::Assign(cur, Expr::bin(BinOp::Add, Expr::Local(cur), Expr::int(1))),
        ];
        full.extend(body);
        out.push(Stmt::While {
            cond: Expr::bin(BinOp::Lt, Expr::Local(cur), Expr::Local(len)),
            body: full,
            bound: Bound::Guarded,
        });
        Ok(())
    }

    fn ret(&mut self, arg: Option<&Expression<'_>>, span: Span, out: &mut Block) -> R<()> {
        let scalar = self.sig.returns;
        let Some(arg) = arg else {
            let fields = if scalar.is_some() {
                vec![("value".to_owned(), Expr::Lit(Literal::Null))]
            } else {
                Vec::new()
            };
            out.push(Stmt::Emit("output".to_owned(), fields));
            out.push(Stmt::Return);
            return Ok(());
        };
        let arg = strip_parens(arg);
        // `return sprig.event(name, {fields})`.
        if let Expression::CallExpression(c) = arg
            && self.callee_path(&c.callee).as_deref() == Some(&["sprig", "event"])
        {
            let name = match c.arguments.first().and_then(Argument::as_expression) {
                Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
                _ => return self.stop("js.event", c.span, "the event's name must be a literal"),
            };
            let fields = match c.arguments.get(1).and_then(Argument::as_expression) {
                None => Vec::new(),
                Some(Expression::ObjectExpression(o)) => self.fields(o)?,
                Some(other) => {
                    return self.stop(
                        "js.event",
                        other.span(),
                        "the fields must be an object literal",
                    );
                }
            };
            self.emit(&name, fields, span, out)?;
            return Ok(());
        }
        if let Some(ty) = scalar {
            let v = self.val(arg)?;
            let e = self.coerce(v, ty, arg.span())?;
            out.push(Stmt::Emit(
                "output".to_owned(),
                vec![("value".to_owned(), e)],
            ));
            out.push(Stmt::Return);
            return Ok(());
        }
        match arg {
            Expression::ObjectExpression(o) => {
                let fields = self.fields(o)?;
                self.emit("output", fields, span, out)
            }
            other => {
                let v = self.val(other)?;
                if v.ty == Ty::Json {
                    return self.stop(
                        "js.return",
                        other.span(),
                        "a JSON value's members become fields only at run time",
                    );
                }
                self.emit("output", vec![("value".to_owned(), v)], span, out)
            }
        }
    }

    fn fields(&mut self, o: &oxc_ast::ast::ObjectExpression<'_>) -> R<Vec<(String, V)>> {
        let mut out: Vec<(String, V)> = Vec::new();
        for p in &o.properties {
            let ObjectPropertyKind::ObjectProperty(p) = p else {
                return self.stop("js.spread", o.span, "spread in an object literal");
            };
            if p.computed || p.method {
                return self.stop("js.object", p.span, "computed keys and methods");
            }
            let Some(k) = p.key.static_name() else {
                return self.stop("js.object", p.span, "the key must be a name");
            };
            let v = self.val(&p.value)?;
            out.retain(|(n, _)| n.as_str() != k.as_ref());
            out.push((k.into_owned(), v));
        }
        Ok(out)
    }

    fn emit(&mut self, name: &str, fields: Vec<(String, V)>, span: Span, out: &mut Block) -> R<()> {
        let idx = match self.events.iter().position(|(n, _)| n == name) {
            Some(i) => i,
            None => {
                self.events.push((name.to_owned(), Vec::new()));
                self.events.len() - 1
            }
        };
        let mut exprs = Vec::new();
        for (k, v) in fields {
            let ty = if v.ty == Ty::Null { None } else { Some(v.ty) };
            let decl = self.events[idx].1.iter().position(|(n, _)| *n == k);
            match (decl, ty) {
                (None, t) => self.events[idx].1.push((k.clone(), t.unwrap_or(Ty::Null))),
                (Some(i), Some(t)) => {
                    let old = self.events[idx].1[i].1;
                    match old.join(t) {
                        Some(j) => self.events[idx].1[i].1 = j,
                        None => {
                            return self.stop(
                                "js.field_type",
                                span,
                                format!("field {k} is {old} in one place and {t} in another"),
                            );
                        }
                    }
                }
                (Some(_), None) => {}
            }
            exprs.push((k, v));
        }
        let fields: Vec<(String, Expr)> = exprs
            .into_iter()
            .map(|(k, v)| {
                let ty = self.events[idx]
                    .1
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map_or(v.ty, |(_, t)| *t);
                let e = if ty == Ty::Float && v.ty == Ty::Int {
                    Expr::call(Builtin::ToFloat, vec![v.e])
                } else {
                    v.e
                };
                (k, e)
            })
            .collect();
        out.push(Stmt::Emit(name.to_owned(), fields));
        out.push(Stmt::Return);
        Ok(())
    }

    // -- data access --------------------------------------------------------------

    fn callee_path(&self, e: &Expression<'_>) -> Option<Vec<&'static str>> {
        let Expression::StaticMemberExpression(m) = strip_parens(e) else {
            return None;
        };
        let Expression::Identifier(obj) = strip_parens(&m.object) else {
            return None;
        };
        let base: &'static str = match self.lookup(obj.name.as_str()) {
            Some(Var::Module(m)) => m,
            None => match obj.name.as_str() {
                "Math" => "Math",
                "ctx" => "ctx",
                "console" => "console",
                "JSON" => "JSON",
                _ => return None,
            },
            Some(_) => return None,
        };
        let prop: &'static str = match m.property.name.as_str() {
            "sql" => "sql",
            "sqlOne" => "sqlOne",
            "exec" => "exec",
            "reject" => "reject",
            "event" => "event",
            "log" => "log",
            "abs" => "abs",
            "floor" => "floor",
            "ceil" => "ceil",
            "round" => "round",
            "sqrt" => "sqrt",
            "pow" => "pow",
            "max" => "max",
            "min" => "min",
            _ => "?",
        };
        Some(vec![base, prop])
    }

    /// `db.sql | sqlOne | exec(text, [args])`: the data operation, and
    /// what the result is bound as. `None` when `e` isn't one.
    fn db_call(&mut self, e: &Expression<'_>, name: &str, out: &mut Block) -> R<Option<Var>> {
        let Expression::CallExpression(c) = strip_parens(e) else {
            return Ok(None);
        };
        let Some(path) = self.callee_path(&c.callee) else {
            return Ok(None);
        };
        let kind = match path.as_slice() {
            ["db", "sql"] => "sql",
            ["db", "sqlOne"] => "sqlOne",
            ["db", "exec"] => "exec",
            ["db", _] => {
                return self.stop(
                    "js.db",
                    c.span,
                    "only db.sql, db.sqlOne and db.exec compile",
                );
            }
            _ => return Ok(None),
        };
        let at = self.at(c.span);
        let text = match c.arguments.first().and_then(Argument::as_expression) {
            Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
            Some(Expression::TemplateLiteral(t)) if t.expressions.is_empty() => t
                .quasis
                .iter()
                .filter_map(|q| q.value.cooked.as_ref().map(|c| c.as_str().to_owned()))
                .collect(),
            _ => return self.stop("db.dynamic_sql", c.span, "the SQL text must be a literal"),
        };
        let mut binds = Vec::new();
        match c.arguments.get(1).map(|a| a.as_expression()) {
            None => {}
            Some(Some(Expression::ArrayExpression(a))) => {
                for el in &a.elements {
                    let Some(x) = el.as_expression() else {
                        return self.stop("js.spread", a.span, "spread in the arguments");
                    };
                    let v = self.val(x)?;
                    if v.ty == Ty::Json {
                        return self.stop(
                            "db.json_bind",
                            x.span(),
                            "JSON values can't be bound yet",
                        );
                    }
                    binds.push(v.e);
                }
            }
            Some(_) => {
                return self.stop("js.db", c.span, "the arguments must be an array literal");
            }
        }
        if c.arguments.len() > 2 {
            return self.stop("js.db", c.span, "db calls take a text and an array");
        }
        let want = if kind == "exec" {
            Want::Write
        } else {
            Want::Read
        };
        let bound = match sql::bind(&text, want, self.cat, &at) {
            Ok(b) => b,
            Err(r) => {
                self.reasons.push(r);
                return Err(Stop);
            }
        };
        if bound.binds != binds.len() {
            self.fatal = Some(sprig_common::SprigError::new(
                sprig_common::SprigCode::LogicSignatureMismatch,
                format!(
                    "{at}: the SQL has {} placeholders but {} arguments",
                    bound.binds,
                    binds.len()
                ),
            ));
            return Err(Stop);
        }
        self.tmp += 1;
        let base = if name.is_empty() {
            format!("db{}", self.tmp)
        } else {
            name.to_owned()
        };
        let (shape, var) = match kind {
            "sqlOne" => {
                let found = self.f.local(&format!("{base}_found"), Ty::Bool);
                let cols: Vec<(String, LocalId)> = bound
                    .columns
                    .iter()
                    .map(|(c, t)| (c.clone(), self.f.local(&format!("{base}_{c}"), *t)))
                    .collect();
                (
                    SqlShape::One {
                        found,
                        into: cols.clone(),
                    },
                    Var::Row { found, cols },
                )
            }
            "sql" => {
                let into = self.f.local(&format!("{base}_rows"), Ty::Json);
                (
                    SqlShape::Many {
                        into,
                        cols: bound.columns.clone(),
                    },
                    Var::Rows {
                        local: into,
                        cols: bound.columns.clone(),
                    },
                )
            }
            _ => {
                let affected = self.f.local(&format!("{base}_affected"), Ty::Int);
                (SqlShape::Exec { affected }, Var::Exec { affected })
            }
        };
        let guards = std::mem::take(&mut self.guards);
        out.extend(self.guard_stmts(guards));
        out.push(Stmt::Db(DbOp::Sql {
            stmt: bound.stmt,
            binds,
            shape,
            origin: at,
        }));
        if let Var::Row { found, .. } = &var {
            self.facts.remove(&Key::L(*found));
        }
        Ok(Some(var))
    }
}

fn strip_parens<'a, 'b>(e: &'b Expression<'a>) -> &'b Expression<'a> {
    match e {
        Expression::ParenthesizedExpression(p) => strip_parens(&p.expression),
        other => other,
    }
}

impl L<'_, '_> {
    // -- expressions ----------------------------------------------------------------

    /// A condition, as `Bool` with JavaScript's truthiness.
    fn test(&mut self, e: &Expression<'_>) -> R<Expr> {
        if let Some(found) = self.row_of(e) {
            return Ok(Expr::Local(found));
        }
        let v = self.val(e)?;
        self.truthy(v, e.span())
    }

    /// The `found` flag when `e` names a `db.sqlOne` row.
    fn row_of(&self, e: &Expression<'_>) -> Option<LocalId> {
        match strip_parens(e) {
            Expression::Identifier(id) => match self.lookup(id.name.as_str()) {
                Some(Var::Row { found, .. }) => Some(found),
                _ => None,
            },
            _ => None,
        }
    }

    /// `row === null`, `row != null`, `!row`: tests of the row's presence.
    fn row_test(&self, e: &Expression<'_>) -> Option<Expr> {
        match strip_parens(e) {
            Expression::UnaryExpression(u) if u.operator == UnaryOperator::LogicalNot => {
                let f = self.row_of(&u.argument)?;
                Some(Expr::Unary(UnOp::Not, Box::new(Expr::Local(f))))
            }
            Expression::BinaryExpression(b) => {
                let is_null = |x: &Expression<'_>| match strip_parens(x) {
                    Expression::NullLiteral(_) => true,
                    Expression::Identifier(u) => u.name.as_str() == "undefined",
                    _ => false,
                };
                let f = if is_null(&b.right) {
                    self.row_of(&b.left)?
                } else if is_null(&b.left) {
                    self.row_of(&b.right)?
                } else {
                    return None;
                };
                match b.operator {
                    BinaryOperator::StrictEquality | BinaryOperator::Equality => {
                        Some(Expr::Unary(UnOp::Not, Box::new(Expr::Local(f))))
                    }
                    BinaryOperator::StrictInequality | BinaryOperator::Inequality => {
                        Some(Expr::Local(f))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn truthy(&mut self, v: V, span: Span) -> R<Expr> {
        Ok(match v.ty {
            Ty::Bool | Ty::Int | Ty::Float => Expr::call(Builtin::Truthy, vec![v.e]),
            Ty::Null => Expr::bool(false),
            // `''` is false in JavaScript.
            Ty::Str => Expr::bin(
                BinOp::And,
                Expr::call(Builtin::Present, vec![v.e.clone()]),
                Expr::bin(BinOp::Ne, Expr::call(Builtin::Len, vec![v.e]), Expr::int(0)),
            ),
            Ty::Json => match &v.e {
                Expr::ObjectLit(_) | Expr::ArrayLit(_) => Expr::bool(true),
                _ => {
                    return self.stop(
                        "js.json_truthiness",
                        span,
                        "the truth of a JSON value depends on its kind at run time",
                    );
                }
            },
        })
    }

    /// Facts that hold when `e` is `truth`.
    fn facts_if(&self, e: &Expression<'_>, truth: bool) -> BTreeSet<Key> {
        let mut s = BTreeSet::new();
        match strip_parens(e) {
            Expression::UnaryExpression(u) if u.operator == UnaryOperator::LogicalNot => {
                return self.facts_if(&u.argument, !truth);
            }
            Expression::LogicalExpression(l) => match (l.operator, truth) {
                (LogicalOperator::And, true) | (LogicalOperator::Or, false) => {
                    s.extend(self.facts_if(&l.left, truth));
                    s.extend(self.facts_if(&l.right, truth));
                }
                _ => {}
            },
            Expression::BinaryExpression(b) => {
                let (x, null) = match (strip_parens(&b.left), strip_parens(&b.right)) {
                    (x, Expression::NullLiteral(_)) | (Expression::NullLiteral(_), x) => (x, true),
                    (x, Expression::Identifier(u)) if u.name.as_str() == "undefined" => (x, true),
                    _ => return s,
                };
                let not_null_when = match b.operator {
                    BinaryOperator::StrictInequality | BinaryOperator::Inequality => true,
                    BinaryOperator::StrictEquality | BinaryOperator::Equality => false,
                    _ => return s,
                };
                if null
                    && not_null_when == truth
                    && let Expression::Identifier(id) = x
                    && let Some(k) = self.lookup(id.name.as_str()).as_ref().and_then(Self::key)
                {
                    s.insert(k);
                }
            }
            Expression::Identifier(id) if truth => {
                if let Some(k) = self.lookup(id.name.as_str()).as_ref().and_then(Self::key) {
                    s.insert(k);
                }
            }
            _ => {}
        }
        s
    }

    /// Lowers `e` with `facts` added and as a conditionally evaluated
    /// operand.
    fn cond_val(&mut self, e: &Expression<'_>, facts: BTreeSet<Key>) -> R<V> {
        let saved = self.facts.clone();
        self.facts.extend(facts);
        self.cond += 1;
        let r = self.val(e);
        self.cond -= 1;
        self.facts = saved;
        r
    }

    #[allow(clippy::too_many_lines)]
    fn val(&mut self, e: &Expression<'_>) -> R<V> {
        if let Some(t) = self.row_test(e) {
            return Ok(V {
                e: t,
                ty: Ty::Bool,
                nn: true,
            });
        }
        match e {
            Expression::ParenthesizedExpression(p) => self.val(&p.expression),
            Expression::NullLiteral(_) => Ok(V::null()),
            Expression::BooleanLiteral(b) => Ok(V::lit(Literal::Bool(b.value))),
            Expression::NumericLiteral(n) => Ok(V::lit(number(n.value))),
            Expression::StringLiteral(s) => Ok(V::lit(Literal::Str(s.value.as_str().to_owned()))),
            Expression::TemplateLiteral(t) => {
                let mut acc: Option<Expr> = None;
                for (i, q) in t.quasis.iter().enumerate() {
                    let Some(text) = q.value.cooked.as_ref() else {
                        return self.stop("js.template", t.span, "an invalid escape");
                    };
                    let mut parts = Vec::new();
                    if !text.as_str().is_empty() {
                        parts.push(Expr::Lit(Literal::Str(text.as_str().to_owned())));
                    }
                    if let Some(x) = t.expressions.get(i) {
                        let v = self.val(x)?;
                        parts.push(self.text_of(v, x.span())?);
                    }
                    for p in parts {
                        acc = Some(match acc {
                            None => p,
                            Some(a) => Expr::bin(BinOp::Concat, a, p),
                        });
                    }
                }
                Ok(V {
                    e: acc.unwrap_or(Expr::Lit(Literal::Str(String::new()))),
                    ty: Ty::Str,
                    nn: true,
                })
            }
            Expression::Identifier(id) => {
                let name = id.name.as_str();
                match self.lookup(name) {
                    Some(Var::Param(i)) => Ok(V {
                        e: Expr::Param(i),
                        ty: self.f.params[i as usize].ty,
                        nn: self.facts.contains(&Key::P(i)),
                    }),
                    Some(Var::Local(l)) => Ok(V {
                        e: Expr::Local(l),
                        ty: self.f.local_def(l).ty,
                        nn: self.facts.contains(&Key::L(l)),
                    }),
                    Some(Var::Rows { local, .. }) => Ok(V {
                        e: Expr::Local(local),
                        ty: Ty::Json,
                        nn: true,
                    }),
                    Some(
                        Var::Row { .. } | Var::Exec { .. } | Var::Elem { .. } | Var::Module(_),
                    ) => self.stop(
                        "js.value",
                        id.span,
                        format!("{name} can't be used as a value in the compiled tier"),
                    ),
                    None if name == "undefined" => Ok(V::null()),
                    None => self.stop("js.unknown_name", id.span, format!("unknown name {name}")),
                }
            }
            Expression::StaticMemberExpression(m) => {
                let prop = m.property.name.as_str();
                if let Expression::Identifier(obj) = strip_parens(&m.object)
                    && let Some(var) = self.lookup(obj.name.as_str())
                {
                    match var {
                        Var::Row { found, cols } => {
                            let Some((_, l)) = cols.iter().find(|(c, _)| c == prop).cloned() else {
                                return self.stop(
                                    "js.member",
                                    m.span,
                                    format!("the row has no column {prop}"),
                                );
                            };
                            if !self.facts.contains(&Key::L(found)) {
                                if self.cond > 0 {
                                    return self.stop(
                                        "js.null_row",
                                        m.span,
                                        format!("{} may be null here", obj.name.as_str()),
                                    );
                                }
                                self.guards.push((found, prop.to_owned()));
                            }
                            return Ok(V {
                                e: Expr::Local(l),
                                ty: self.f.local_def(l).ty,
                                nn: false,
                            });
                        }
                        Var::Exec { affected } if prop == "affected" => {
                            return Ok(V {
                                e: Expr::Local(affected),
                                ty: Ty::Int,
                                nn: true,
                            });
                        }
                        Var::Elem { local, cols } => {
                            let Some((_, ty)) = cols.iter().find(|(c, _)| c == prop).cloned()
                            else {
                                return self.stop(
                                    "js.member",
                                    m.span,
                                    format!("the rows have no column {prop}"),
                                );
                            };
                            let path = Expr::Path(
                                Box::new(Expr::Local(local)),
                                vec![PathSeg::Key(prop.to_owned())],
                            );
                            let (e, ty) = match ty {
                                Ty::Int | Ty::Float => {
                                    (Expr::call(Builtin::JsonNum, vec![path]), Ty::Float)
                                }
                                Ty::Str => (Expr::call(Builtin::JsonStr, vec![path]), Ty::Str),
                                _ => (path, Ty::Json),
                            };
                            return Ok(V { e, ty, nn: false });
                        }
                        Var::Rows { local, .. } if prop == "length" => {
                            return Ok(V {
                                e: Expr::bin(
                                    BinOp::Coalesce,
                                    Expr::call(Builtin::JsonLength, vec![Expr::Local(local)]),
                                    Expr::int(0),
                                ),
                                ty: Ty::Int,
                                nn: true,
                            });
                        }
                        _ => {}
                    }
                }
                let v = self.val(&m.object)?;
                match (v.ty, prop) {
                    (Ty::Str, "length") => {
                        self.need_nn(&v, m.span)?;
                        Ok(V {
                            e: Expr::call(Builtin::Len, vec![v.e]),
                            ty: Ty::Int,
                            nn: true,
                        })
                    }
                    _ => self.stop("js.member", m.span, format!("reading .{prop} here")),
                }
            }
            Expression::UnaryExpression(u) => {
                let v = self.val(&u.argument)?;
                match u.operator {
                    UnaryOperator::LogicalNot => {
                        let t = self.truthy(v, u.span)?;
                        Ok(V {
                            e: Expr::Unary(UnOp::Not, Box::new(t)),
                            ty: Ty::Bool,
                            nn: true,
                        })
                    }
                    UnaryOperator::UnaryNegation => {
                        let v = self.num(v, u.span)?;
                        Ok(V {
                            ty: v.ty,
                            e: Expr::Unary(UnOp::Neg, Box::new(v.e)),
                            nn: true,
                        })
                    }
                    UnaryOperator::UnaryPlus => self.num(v, u.span),
                    _ => self.stop("js.operator", u.span, "this operator"),
                }
            }
            Expression::BinaryExpression(b) => {
                let x = self.val(&b.left)?;
                let y = self.val(&b.right)?;
                self.binary(b.operator, x, y, b.span)
            }
            Expression::LogicalExpression(l) => {
                let x = self.val(&l.left)?;
                let y = match l.operator {
                    LogicalOperator::Or => {
                        self.cond_val(&l.right, self.facts_if(&l.left, false))?
                    }
                    LogicalOperator::And => {
                        self.cond_val(&l.right, self.facts_if(&l.left, true))?
                    }
                    LogicalOperator::Coalesce => self.cond_val(&l.right, BTreeSet::new())?,
                };
                if l.operator == LogicalOperator::Coalesce {
                    let ty = self.join(x.ty, y.ty, l.span)?;
                    let (a, b) = (
                        self.coerce(x.clone(), ty, l.span)?,
                        self.coerce(y.clone(), ty, l.span)?,
                    );
                    return Ok(V {
                        e: Expr::bin(BinOp::Coalesce, a, b),
                        ty,
                        nn: x.nn || y.nn,
                    });
                }
                if x.ty == Ty::Bool && y.ty == Ty::Bool {
                    let op = if l.operator == LogicalOperator::Or {
                        BinOp::Or
                    } else {
                        BinOp::And
                    };
                    let tx = self.truthy(x.clone(), l.span)?;
                    let ty_ = self.truthy(y.clone(), l.span)?;
                    // A Bool operand that is `NULL` would be returned as
                    // itself in JavaScript; only non-null ones fold.
                    if x.nn && y.nn {
                        return Ok(V {
                            e: Expr::bin(op, tx, ty_),
                            ty: Ty::Bool,
                            nn: true,
                        });
                    }
                }
                // The operand values themselves, as JavaScript returns.
                let ty = self.join(x.ty, y.ty, l.span)?;
                let t = self.truthy(x.clone(), l.span)?;
                let (a, b) = (
                    self.coerce(x.clone(), ty, l.span)?,
                    self.coerce(y.clone(), ty, l.span)?,
                );
                let (e, nn) = if l.operator == LogicalOperator::Or {
                    (Expr::Cond(Box::new(t), Box::new(a), Box::new(b)), y.nn)
                } else {
                    (
                        Expr::Cond(Box::new(t), Box::new(b), Box::new(a)),
                        x.nn && y.nn,
                    )
                };
                Ok(V { e, ty, nn })
            }
            Expression::ConditionalExpression(c) => {
                let t = self.test(&c.test)?;
                let a = self.cond_val(&c.consequent, self.facts_if(&c.test, true))?;
                let b = self.cond_val(&c.alternate, self.facts_if(&c.test, false))?;
                let ty = self.join(a.ty, b.ty, c.span)?;
                let nn = a.nn && b.nn;
                let (a, b) = (self.coerce(a, ty, c.span)?, self.coerce(b, ty, c.span)?);
                Ok(V {
                    e: Expr::Cond(Box::new(t), Box::new(a), Box::new(b)),
                    ty,
                    nn,
                })
            }
            Expression::ObjectExpression(o) => {
                let fields = self.fields(o)?;
                let mut m = Vec::new();
                for (k, v) in fields {
                    let e = v.e;
                    m.push((k, e));
                }
                Ok(V {
                    e: Expr::ObjectLit(m),
                    ty: Ty::Json,
                    nn: true,
                })
            }
            Expression::ArrayExpression(a) => {
                let mut items = Vec::new();
                for el in &a.elements {
                    let Some(x) = el.as_expression() else {
                        return self.stop("js.spread", a.span, "spread or holes in an array");
                    };
                    let v = self.val(x)?;
                    items.push(v.e);
                }
                Ok(V {
                    e: Expr::ArrayLit(items),
                    ty: Ty::Json,
                    nn: true,
                })
            }
            Expression::CallExpression(c) => self.call(c),
            Expression::ChainExpression(c) => {
                self.stop("js.optional_chain", c.span, "optional chaining")
            }
            other => self.stop(
                "js.expression",
                other.span(),
                "this expression runs in the engine tier",
            ),
        }
    }

    fn call(&mut self, c: &oxc_ast::ast::CallExpression<'_>) -> R<V> {
        let mut args = Vec::new();
        for a in &c.arguments {
            let Some(x) = a.as_expression() else {
                return self.stop("js.spread", c.span, "spread arguments");
            };
            args.push(x);
        }
        if let Some(path) = self.callee_path(&c.callee) {
            let b = match path.as_slice() {
                ["Math", "abs"] => Some(Builtin::Abs),
                ["Math", "floor"] => Some(Builtin::Floor),
                ["Math", "ceil"] => Some(Builtin::Ceil),
                ["Math", "round"] => Some(Builtin::Round),
                ["Math", "sqrt"] => Some(Builtin::Sqrt),
                ["Math", "pow"] => Some(Builtin::Pow),
                ["Math", "max"] => Some(Builtin::Max),
                ["Math", "min"] => Some(Builtin::Min),
                _ => None,
            };
            let Some(b) = b else {
                return self.stop("js.call", c.span, "this call runs in the engine tier");
            };
            let want = match b {
                Builtin::Pow => 2,
                Builtin::Max | Builtin::Min => args.len().max(1),
                _ => 1,
            };
            if args.len() != want {
                return self.stop("js.call", c.span, "the number of arguments");
            }
            let mut xs = Vec::new();
            let mut ty = Ty::Int;
            for a in args {
                let v = self.val(a)?;
                let v = self.num(v, a.span())?;
                if v.ty == Ty::Float {
                    ty = Ty::Float;
                }
                xs.push(v);
            }
            let e = match b {
                Builtin::Abs => Expr::call(b, vec![xs.remove(0).e]),
                Builtin::Max | Builtin::Min => {
                    let es = xs
                        .into_iter()
                        .map(|v| {
                            if ty == Ty::Float && v.ty == Ty::Int {
                                Expr::call(Builtin::ToFloat, vec![v.e])
                            } else {
                                v.e
                            }
                        })
                        .collect();
                    Expr::call(b, es)
                }
                _ => Expr::call(
                    b,
                    xs.into_iter()
                        .map(|v| {
                            if v.ty == Ty::Int {
                                Expr::call(Builtin::ToFloat, vec![v.e])
                            } else {
                                v.e
                            }
                        })
                        .collect(),
                ),
            };
            let out_ty = match b {
                Builtin::Abs | Builtin::Max | Builtin::Min => ty,
                _ => Ty::Float,
            };
            return Ok(V {
                e,
                ty: out_ty,
                nn: b != Builtin::Sqrt,
            });
        }
        // `String(x)`.
        if let Expression::Identifier(id) = strip_parens(&c.callee)
            && id.name.as_str() == "String"
            && self.lookup("String").is_none()
            && args.len() == 1
        {
            let v = self.val(args[0])?;
            let e = self.text_of(v, c.span)?;
            return Ok(V {
                e,
                ty: Ty::Str,
                nn: true,
            });
        }
        // String methods.
        if let Expression::StaticMemberExpression(m) = strip_parens(&c.callee) {
            let method = m.property.name.as_str();
            let b = match method {
                "toUpperCase" => Some((Builtin::Upper, 0)),
                "toLowerCase" => Some((Builtin::Lower, 0)),
                "includes" => Some((Builtin::Contains, 1)),
                "startsWith" => Some((Builtin::StartsWith, 1)),
                "endsWith" => Some((Builtin::EndsWith, 1)),
                "indexOf" => Some((Builtin::IndexOf, 1)),
                _ => None,
            };
            if let Some((b, n)) = b
                && args.len() == n
            {
                let s = self.val(&m.object)?;
                if s.ty != Ty::Str {
                    return self.stop("js.call", c.span, format!("{method} on a non-string"));
                }
                self.need_nn(&s, m.span)?;
                let mut es = vec![s.e];
                for a in args {
                    let v = self.val(a)?;
                    if v.ty != Ty::Str || !v.nn {
                        return self.stop(
                            "js.call",
                            a.span(),
                            format!("{method} takes a non-null string here"),
                        );
                    }
                    es.push(v.e);
                }
                let ty = match b {
                    Builtin::Upper | Builtin::Lower => Ty::Str,
                    Builtin::IndexOf => Ty::Int,
                    _ => Ty::Bool,
                };
                return Ok(V {
                    e: Expr::call(b, es),
                    ty,
                    nn: true,
                });
            }
        }
        self.stop("js.call", c.span, "this call runs in the engine tier")
    }

    fn need_nn(&mut self, v: &V, span: Span) -> R<()> {
        if v.nn {
            Ok(())
        } else {
            self.stop(
                "js.nullable_receiver",
                span,
                "the value may be null here (JavaScript would throw a TypeError)",
            )
        }
    }

    /// A number operand: `null` reads as 0, as in JavaScript.
    fn num(&mut self, v: V, span: Span) -> R<V> {
        match v.ty {
            Ty::Int | Ty::Float => Ok(if v.nn {
                v
            } else {
                let zero = if v.ty == Ty::Int {
                    Expr::int(0)
                } else {
                    Expr::Lit(Literal::Float(0.0))
                };
                V {
                    e: Expr::bin(BinOp::Coalesce, v.e, zero),
                    ty: v.ty,
                    nn: true,
                }
            }),
            Ty::Null => Ok(V::lit(Literal::Int(0))),
            other => self.stop(
                "js.dynamic_types",
                span,
                format!("arithmetic on a {other} value"),
            ),
        }
    }

    fn widen_pair(a: V, b: V) -> (Expr, Expr, Ty) {
        if a.ty == Ty::Int && b.ty == Ty::Int {
            (a.e, b.e, Ty::Int)
        } else {
            let f = |v: V| {
                if v.ty == Ty::Int {
                    Expr::call(Builtin::ToFloat, vec![v.e])
                } else {
                    v.e
                }
            };
            (f(a), f(b), Ty::Float)
        }
    }

    #[allow(clippy::too_many_lines)]
    fn binary(&mut self, op: BinaryOperator, x: V, y: V, span: Span) -> R<V> {
        use BinaryOperator as B;
        match op {
            B::Addition if x.ty == Ty::Str || y.ty == Ty::Str => {
                let a = self.text_of(x, span)?;
                let b = self.text_of(y, span)?;
                Ok(V {
                    e: Expr::bin(BinOp::Concat, a, b),
                    ty: Ty::Str,
                    nn: true,
                })
            }
            B::Addition | B::Subtraction | B::Multiplication | B::Remainder => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let (a, b, ty) = Self::widen_pair(a, b);
                let o = match op {
                    B::Addition => BinOp::Add,
                    B::Subtraction => BinOp::Sub,
                    B::Multiplication => BinOp::Mul,
                    _ => BinOp::Mod,
                };
                Ok(V {
                    e: Expr::bin(o, a, b),
                    ty,
                    nn: true,
                })
            }
            B::Division => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let (a, b, _) = Self::widen_pair(a, b);
                Ok(V {
                    e: Expr::bin(BinOp::Div, a, b),
                    ty: Ty::Float,
                    nn: true,
                })
            }
            B::Exponential => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let f = |v: V| {
                    if v.ty == Ty::Int {
                        Expr::call(Builtin::ToFloat, vec![v.e])
                    } else {
                        v.e
                    }
                };
                Ok(V {
                    e: Expr::call(Builtin::Pow, vec![f(a), f(b)]),
                    ty: Ty::Float,
                    nn: true,
                })
            }
            B::StrictEquality | B::StrictInequality | B::Equality | B::Inequality => {
                let negate = matches!(op, B::StrictInequality | B::Inequality);
                let strict = matches!(op, B::StrictEquality | B::StrictInequality);
                let same_kind = x.ty == y.ty
                    || (x.ty.is_number() && y.ty.is_number())
                    || x.ty == Ty::Null
                    || y.ty == Ty::Null;
                if x.ty == Ty::Json || y.ty == Ty::Json {
                    return self.stop(
                        "js.dynamic_types",
                        span,
                        "comparing JSON values depends on their kind at run time",
                    );
                }
                let e = if same_kind {
                    let (a, b) = if x.ty.is_number() && y.ty.is_number() {
                        let (a, b, _) = Self::widen_pair(x, y);
                        (a, b)
                    } else {
                        (x.e, y.e)
                    };
                    Expr::bin(BinOp::Eq, a, b)
                } else if strict {
                    Expr::bool(false)
                } else {
                    return self.stop(
                        "js.loose_equality",
                        span,
                        "== between values of different kinds converts them",
                    );
                };
                Ok(V {
                    e: if negate {
                        Expr::Unary(UnOp::Not, Box::new(e))
                    } else {
                        e
                    },
                    ty: Ty::Bool,
                    nn: true,
                })
            }
            B::LessThan | B::LessEqualThan | B::GreaterThan | B::GreaterEqualThan => {
                let o = match op {
                    B::LessThan => BinOp::Lt,
                    B::LessEqualThan => BinOp::Le,
                    B::GreaterThan => BinOp::Gt,
                    _ => BinOp::Ge,
                };
                let (a, b) = if x.ty == Ty::Str && y.ty == Ty::Str && x.nn && y.nn {
                    (x.e, y.e)
                } else if matches!(x.ty, Ty::Int | Ty::Float | Ty::Null)
                    && matches!(y.ty, Ty::Int | Ty::Float | Ty::Null)
                {
                    let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                    let (a, b, _) = Self::widen_pair(a, b);
                    (a, b)
                } else {
                    return self.stop(
                        "js.compare",
                        span,
                        "this comparison converts its operands at run time",
                    );
                };
                Ok(V {
                    e: Expr::bin(o, a, b),
                    ty: Ty::Bool,
                    nn: true,
                })
            }
            _ => self.stop("js.operator", span, "this operator runs in the engine tier"),
        }
    }

    /// Text for `+` and templates: JavaScript's `String(x)`.
    fn text_of(&mut self, v: V, span: Span) -> R<Expr> {
        Ok(match v.ty {
            Ty::Str => v.e,
            Ty::Null => Expr::Lit(Literal::Str("null".to_owned())),
            Ty::Int | Ty::Float | Ty::Bool => {
                if v.nn {
                    Expr::call(Builtin::ToStr, vec![v.e])
                } else {
                    Expr::bin(
                        BinOp::Coalesce,
                        Expr::call(Builtin::ToStr, vec![v.e]),
                        Expr::Lit(Literal::Str("null".to_owned())),
                    )
                }
            }
            Ty::Json => {
                return self.stop(
                    "js.dynamic_types",
                    span,
                    "a JSON value as text depends on its kind",
                );
            }
        })
    }

    fn join(&mut self, a: Ty, b: Ty, span: Span) -> R<Ty> {
        match a.join(b) {
            Some(Ty::Null) => Ok(Ty::Null),
            Some(t) => Ok(t),
            None => self.stop(
                "js.types",
                span,
                format!("a value is {a} on one path and {b} on another"),
            ),
        }
    }

    fn coerce(&mut self, v: V, ty: Ty, span: Span) -> R<Expr> {
        if v.ty == ty || v.ty == Ty::Null {
            return Ok(v.e);
        }
        if ty == Ty::Float && v.ty == Ty::Int {
            return Ok(Expr::call(Builtin::ToFloat, vec![v.e]));
        }
        if ty == Ty::Null {
            // A local first holding null: its type comes from this value.
            return self.stop("js.types", span, "a value of a new type");
        }
        self.stop(
            "js.types",
            span,
            format!("a {} value where {ty} is expected", v.ty),
        )
    }
}

/// A JavaScript number literal: whole numbers within ±2^53 are `Int`.
fn number(x: f64) -> Literal {
    if x.fract() == 0.0 && x.abs() < 9_007_199_254_740_992.0 {
        Literal::Int(x as i64)
    } else {
        Literal::Float(x)
    }
}

#[cfg(test)]
mod tests;
