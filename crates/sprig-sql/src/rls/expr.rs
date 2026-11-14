//! Policy expressions: validation when a policy is created, and rendering for
//! one table reference with the session's values bound.
//!
//! ```text
//! validate(text):
//!   one expression (sqlparser-rs, MySQL dialect, on the parse view)
//!   no ?, @var, @@var; no __sprig_ / _sprig_ name; no stored function
//!   (a qualified call); functions: deterministic built-ins and the
//!   session functions; no INTO, FOR UPDATE; subquery tables listed
//!   (the gateway resolves them in the tenant, refuses system schemas,
//!   and checks policy cycles)
//! render(text, table, qualifier, binder):
//!   top-level column c          → qualifier.c
//!   table.c (not shadowed)      → qualifier.c, also inside subqueries
//!   SPRIG_CLAIM('k') and friends → binder's text (literals for the
//!   gateway's rewrite, @sprig_rls lookups for the MariaDB triggers)
//! ```
//!
//! Unqualified columns inside a subquery belong to the subquery's tables
//! (SQL's scoping): a policy names the protected table's columns there as
//! `table.c`.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sprig_common::{SprigCode, SprigError};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Query, SetExpr, TableFactor, Value, Visit, Visitor,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token as SqlToken;

use super::stmt::TableName;
use super::text::{Edit, Positions, Toks, apply};
use crate::lex::{self, LexOptions, TokenKind, ident_value};
use crate::names::rewrite::SystemSchema;
use crate::names::rewrite::ast::parse_view;

/// 39364 `ER_SPRIG_RLS_POLICY_INVALID`.
#[must_use]
pub fn invalid(why: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::RlsPolicyInvalid,
        format!("ER_SPRIG_RLS_POLICY_INVALID: {why}"),
    )
}

/// The session functions a policy may call.
pub const SESSION_FUNCTIONS: &[&str] = &[
    "CURRENT_USER",
    "SPRIG_USER_ID",
    "SPRIG_TENANT_ID",
    "SPRIG_HOME_TENANT_ID",
    "SPRIG_TENANT_IN_SUBTREE",
    "SPRIG_TENANT_MANAGED",
    "SPRIG_USER_ATTR",
    "SPRIG_CLAIM",
    "SPRIG_SESSION_SPACE",
];

/// Session functions taking one column or expression (a membership test).
const MEMBERSHIP: &[&str] = &["SPRIG_TENANT_IN_SUBTREE", "SPRIG_TENANT_MANAGED"];

/// Session functions taking one string literal.
const KEYED: &[&str] = &["SPRIG_USER_ATTR", "SPRIG_CLAIM"];

/// Deterministic MariaDB built-ins a policy may call. Anything else
/// (`NOW()`, `RAND()`, `UUID()`, `USER()`, stored functions, ...) is
/// refused with 39364.
const DETERMINISTIC: &[&str] = &[
    // control flow and comparison
    "COALESCE",
    "IFNULL",
    "NULLIF",
    "IF",
    "GREATEST",
    "LEAST",
    "ISNULL",
    "INTERVAL",
    "STRCMP",
    "NVL",
    "NVL2",
    // strings
    "CONCAT",
    "CONCAT_WS",
    "LOWER",
    "UPPER",
    "LCASE",
    "UCASE",
    "LENGTH",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "OCTET_LENGTH",
    "BIT_LENGTH",
    "SUBSTRING",
    "SUBSTR",
    "MID",
    "LEFT",
    "RIGHT",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "REVERSE",
    "LPAD",
    "RPAD",
    "INSTR",
    "LOCATE",
    "POSITION",
    "FIND_IN_SET",
    "FIELD",
    "ELT",
    "SUBSTRING_INDEX",
    "REPEAT",
    "SPACE",
    "ASCII",
    "ORD",
    "HEX",
    "UNHEX",
    "BIN",
    "OCT",
    "CONV",
    "QUOTE",
    "SOUNDEX",
    "INSERT",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REGEXP_INSTR",
    "MD5",
    "SHA1",
    "SHA",
    "SHA2",
    "CRC32",
    "TO_BASE64",
    "FROM_BASE64",
    "COLLATION",
    "CHARSET",
    "BINARY",
    "CAST",
    "CONVERT",
    "CHAR",
    // numbers
    "ABS",
    "CEIL",
    "CEILING",
    "FLOOR",
    "ROUND",
    "TRUNCATE",
    "MOD",
    "SIGN",
    "SQRT",
    "POW",
    "POWER",
    "EXP",
    "LN",
    "LOG",
    "LOG2",
    "LOG10",
    "PI",
    "DEGREES",
    "RADIANS",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "ATAN2",
    "COT",
    // dates of a given value
    "DATE",
    "TIME",
    "YEAR",
    "MONTH",
    "DAY",
    "DAYOFMONTH",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MICROSECOND",
    "DATE_FORMAT",
    "STR_TO_DATE",
    "DATE_ADD",
    "DATE_SUB",
    "ADDDATE",
    "SUBDATE",
    "DATEDIFF",
    "TIMESTAMPDIFF",
    "TIMESTAMPADD",
    "MAKEDATE",
    "MAKETIME",
    "TO_DAYS",
    "FROM_DAYS",
    "LAST_DAY",
    "WEEK",
    "WEEKDAY",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "QUARTER",
    "YEARWEEK",
    "EXTRACT",
    "TIMESTAMP",
    "TIME_TO_SEC",
    "SEC_TO_TIME",
    "DAYNAME",
    "MONTHNAME",
    // JSON
    "JSON_EXTRACT",
    "JSON_VALUE",
    "JSON_UNQUOTE",
    "JSON_QUOTE",
    "JSON_CONTAINS",
    "JSON_CONTAINS_PATH",
    "JSON_LENGTH",
    "JSON_TYPE",
    "JSON_KEYS",
    "JSON_OVERLAPS",
    "JSON_SEARCH",
    "JSON_QUERY",
    "JSON_ARRAY",
    "JSON_OBJECT",
    "JSON_VALID",
    "JSON_DEPTH",
    "JSON_EXISTS",
    // aggregates (inside subqueries)
    "COUNT",
    "SUM",
    "MIN",
    "MAX",
    "AVG",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
];

/// What a valid policy expression reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExprInfo {
    /// Tables named in subqueries, as written.
    pub tables: Vec<TableName>,
    /// Session functions called (upper case).
    pub functions: Vec<String>,
}

fn parse_expr_view(text: &str, lex_opts: LexOptions) -> Result<(String, Expr), SprigError> {
    parse_expr_view_as(text, lex_opts, true)
}

/// `strict`: the policy rules on tokens (no variables, parameters or
/// reserved names); off for texts the rewrite itself produced.
fn parse_expr_view_as(
    text: &str,
    lex_opts: LexOptions,
    strict: bool,
) -> Result<(String, Expr), SprigError> {
    let all = lex::lex(text.as_bytes(), &lex_opts).map_err(|_| invalid("too long"))?;
    for t in all.iter().filter(|_| strict) {
        match t.kind {
            TokenKind::UserVar | TokenKind::SysVar => {
                return Err(invalid("a policy can't read variables"));
            }
            TokenKind::Param => return Err(invalid("a policy can't take parameters")),
            TokenKind::Unterminated | TokenKind::Other => {
                return Err(invalid("the expression doesn't parse"));
            }
            TokenKind::Word | TokenKind::QuotedIdent => {
                let v = ident_value(t.bytes(text.as_bytes()));
                let v = String::from_utf8_lossy(&v).to_ascii_lowercase();
                if v.starts_with("__sprig_") || v.starts_with("_sprig_") {
                    return Err(invalid(format!("'{v}' is reserved")));
                }
            }
            _ => {}
        }
    }
    let view = parse_view(text, &all).ok_or_else(|| invalid("the expression doesn't parse"))?;
    let mut parser = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(&view)
        .map_err(|e| invalid(format!("the expression doesn't parse: {e}")))?;
    let e = parser
        .parse_expr()
        .map_err(|e| invalid(format!("the expression doesn't parse: {e}")))?;
    if parser.peek_token().token != SqlToken::EOF {
        return Err(invalid(
            "the expression doesn't parse: text after the expression",
        ));
    }
    Ok((view, e))
}

fn fn_name(n: &ObjectName) -> Option<String> {
    match n.0.as_slice() {
        [ObjectNamePart::Identifier(i)] => Some(i.value.to_ascii_uppercase()),
        _ => None,
    }
}

fn single_string_arg(args: &FunctionArguments) -> Option<String> {
    let FunctionArguments::List(l) = args else {
        return None;
    };
    match l.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn arg_count(args: &FunctionArguments) -> usize {
    match args {
        FunctionArguments::List(l) => l.args.len(),
        _ => 0,
    }
}

struct Check {
    err: Option<SprigError>,
    tables: Vec<TableName>,
    functions: Vec<String>,
    depth: usize,
}

impl Visitor for Check {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        self.depth += 1;
        if !q.locks.is_empty() || q.for_clause.is_some() {
            self.err = Some(invalid("a policy subquery can't lock rows"));
            return ControlFlow::Break(());
        }
        if let SetExpr::Select(s) = q.body.as_ref()
            && s.into.is_some()
        {
            self.err = Some(invalid("a policy subquery can't write"));
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, n: &ObjectName) -> ControlFlow<()> {
        let parts: Vec<String> =
            n.0.iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .collect();
        let t = match parts.as_slice() {
            [t] => TableName {
                db: None,
                name: t.clone(),
            },
            [d, t] => TableName {
                db: Some(d.clone()),
                name: t.clone(),
            },
            _ => {
                self.err = Some(invalid("a subquery names a table the policy can't use"));
                return ControlFlow::Break(());
            }
        };
        if t.db.as_deref().and_then(SystemSchema::parse).is_some() {
            self.err = Some(invalid("a policy can't read system schemas"));
            return ControlFlow::Break(());
        }
        self.tables.push(t);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
        match f {
            TableFactor::Table { args: None, .. }
            | TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. } => ControlFlow::Continue(()),
            _ => {
                self.err = Some(invalid("a policy subquery reads only tables"));
                ControlFlow::Break(())
            }
        }
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Function(f) => {
                let Some(name) = fn_name(&f.name) else {
                    self.err = Some(invalid("a policy can't call stored functions"));
                    return ControlFlow::Break(());
                };
                if f.over.is_some() {
                    self.err = Some(invalid("a policy can't use window functions"));
                    return ControlFlow::Break(());
                }
                if SESSION_FUNCTIONS.contains(&name.as_str()) {
                    let ok = if KEYED.contains(&name.as_str()) {
                        single_string_arg(&f.args).is_some()
                    } else if MEMBERSHIP.contains(&name.as_str()) {
                        arg_count(&f.args) == 1
                    } else {
                        arg_count(&f.args) == 0
                    };
                    if !ok {
                        self.err = Some(invalid(format!("wrong arguments to {name}()")));
                        return ControlFlow::Break(());
                    }
                    self.functions.push(name);
                } else if name == "UNIX_TIMESTAMP" && arg_count(&f.args) > 0 {
                    // With an argument it is a pure function.
                } else if !DETERMINISTIC.contains(&name.as_str()) {
                    self.err = Some(invalid(format!(
                        "{name}() isn't a deterministic built-in or a session function"
                    )));
                    return ControlFlow::Break(());
                }
            }
            Expr::Identifier(i) if i.quote_style.is_none() => {
                let u = i.value.to_ascii_uppercase();
                if u == "CURRENT_USER" {
                    self.functions.push(u);
                } else if matches!(
                    u.as_str(),
                    "CURRENT_TIMESTAMP"
                        | "CURRENT_DATE"
                        | "CURRENT_TIME"
                        | "LOCALTIME"
                        | "LOCALTIMESTAMP"
                        | "UTC_TIMESTAMP"
                        | "UTC_DATE"
                        | "UTC_TIME"
                        | "CURRENT_ROLE"
                ) {
                    self.err = Some(invalid(format!("{u} isn't deterministic")));
                    return ControlFlow::Break(());
                }
            }
            Expr::CompoundIdentifier(ids) if self.depth == 0 && ids.len() > 3 => {
                self.err = Some(invalid("a column name has too many parts"));
                return ControlFlow::Break(());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Checks a policy expression (see the module docs).
///
/// # Errors
/// 39364 `ER_SPRIG_RLS_POLICY_INVALID`.
pub fn validate(text: &str, lex_opts: &LexOptions) -> Result<ExprInfo, SprigError> {
    let (_, e) = parse_expr_view(text, *lex_opts)?;
    let mut c = Check {
        err: None,
        tables: Vec::new(),
        functions: Vec::new(),
        depth: 0,
    };
    let _ = e.visit(&mut c);
    if let Some(err) = c.err {
        return Err(err);
    }
    Ok(ExprInfo {
        tables: c.tables,
        functions: c.functions,
    })
}

struct Unqualified<'a> {
    pos: &'a Positions<'a>,
    at: Vec<usize>,
}

impl Visitor for Unqualified<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, n: &ObjectName) -> ControlFlow<()> {
        if let [ObjectNamePart::Identifier(i)] = n.0.as_slice()
            && let Some(o) = self.pos.offset(i.span.start)
        {
            self.at.push(o);
        }
        ControlFlow::Continue(())
    }
}

/// `text` with every unqualified table of its subqueries qualified with
/// `db` (the policy's table's database), so the policy reads the same
/// tables whatever database a session has selected. CTE names inside a
/// policy subquery aren't supported (they'd be qualified too).
///
/// # Errors
/// 39364 if the expression doesn't parse.
pub fn qualify_tables(text: &str, lex_opts: &LexOptions, db: &str) -> Result<String, SprigError> {
    let (_, e) = parse_expr_view(text, *lex_opts)?;
    let pos = Positions::new(text);
    let mut u = Unqualified {
        pos: &pos,
        at: Vec::new(),
    };
    let _ = e.visit(&mut u);
    let q = format!("{}.", super::text::quote(db));
    let edits: Vec<Edit> =
        u.at.into_iter()
            .map(|o| Edit {
                start: o,
                end: o,
                with: q.clone(),
            })
            .collect();
    apply(text, &edits).map_err(|_| invalid("overlapping edits"))
}

struct Relations<'a> {
    pos: &'a Positions<'a>,
    /// (start of the database part or the name, database, table).
    at: Vec<(usize, Option<String>, String)>,
}

impl Visitor for Relations<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, n: &ObjectName) -> ControlFlow<()> {
        match n.0.as_slice() {
            [ObjectNamePart::Identifier(i)] => {
                if let Some(o) = self.pos.offset(i.span.start) {
                    self.at.push((o, None, i.value.clone()));
                }
            }
            [ObjectNamePart::Identifier(d), ObjectNamePart::Identifier(t)] => {
                if let Some(o) = self.pos.offset(d.span.start) {
                    self.at.push((o, Some(d.value.clone()), t.value.clone()));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// A rendered expression with its subquery tables' databases replaced:
/// `schema(db, table)` names the physical schema of `table` in logical
/// database `db` (`None`: unqualified).
///
/// # Errors
/// 39364 if the text doesn't parse or a database has no schema.
pub fn map_table_schemas(
    text: &str,
    lex_opts: &LexOptions,
    schema: &dyn Fn(Option<&str>, &str) -> Option<String>,
) -> Result<String, SprigError> {
    let (_, e) = parse_expr_view_as(text, *lex_opts, false)?;
    let all = lex::lex(text.as_bytes(), lex_opts).map_err(|_| invalid("too long"))?;
    let toks = Toks::new(text.as_bytes(), &all);
    let pos = Positions::new(text);
    let mut r = Relations {
        pos: &pos,
        at: Vec::new(),
    };
    let _ = e.visit(&mut r);
    let mut edits = Vec::new();
    for (o, db, table) in r.at {
        let s = schema(db.as_deref(), &table).ok_or_else(|| {
            invalid(format!(
                "no schema for database '{}'",
                db.clone().unwrap_or_default()
            ))
        })?;
        if db.is_none() {
            edits.push(Edit {
                start: o,
                end: o,
                with: format!("{}.", super::text::quote(&s)),
            });
        } else {
            let i = toks
                .at(o)
                .ok_or_else(|| invalid("a table position is unknown"))?;
            edits.push(Edit {
                start: o,
                end: toks.end(i),
                with: super::text::quote(&s),
            });
        }
    }
    apply(text, &edits).map_err(|_| invalid("overlapping edits"))
}

/// How [`render`] binds the session functions.
pub trait Binder {
    /// The text replacing a call of a constant session function (`func`
    /// upper case; `arg` the string argument of `SPRIG_CLAIM` and
    /// `SPRIG_USER_ATTR`).
    ///
    /// # Errors
    /// 39364 for a function the binder can't bind.
    fn value(&self, func: &str, arg: Option<&str>) -> Result<String, SprigError>;

    /// The texts around the argument of a membership function
    /// (`SPRIG_TENANT_IN_SUBTREE(x)` → `(x IN (77, 78))` is `("(", " IN
    /// (77, 78))")`).
    ///
    /// # Errors
    /// 39364 for a function the binder can't bind.
    fn membership(&self, func: &str) -> Result<(String, String), SprigError>;
}

struct Render<'a> {
    pos: &'a Positions<'a>,
    toks: &'a Toks<'a>,
    table: &'a str,
    qual: &'a str,
    binder: &'a dyn Binder,
    nbe: bool,
    edits: Vec<Edit>,
    /// Names exposed by enclosing subqueries' FROM clauses (lower case).
    scopes: Vec<HashSet<String>>,
    err: Option<SprigError>,
}

fn exposed(q: &Query, out: &mut HashSet<String>) {
    fn factor(f: &TableFactor, out: &mut HashSet<String>) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                if let Some(a) = alias {
                    out.insert(a.name.value.to_ascii_lowercase());
                } else if let Some(ObjectNamePart::Identifier(i)) = name.0.last() {
                    out.insert(i.value.to_ascii_lowercase());
                }
            }
            TableFactor::Derived { alias: Some(a), .. } => {
                out.insert(a.name.value.to_ascii_lowercase());
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                factor(&table_with_joins.relation, out);
                for j in &table_with_joins.joins {
                    factor(&j.relation, out);
                }
            }
            _ => {}
        }
    }
    fn set(s: &SetExpr, out: &mut HashSet<String>) {
        match s {
            SetExpr::Select(sel) => {
                for t in &sel.from {
                    factor(&t.relation, out);
                    for j in &t.joins {
                        factor(&j.relation, out);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                set(left, out);
                set(right, out);
            }
            SetExpr::Query(q) => set(&q.body, out),
            _ => {}
        }
    }
    set(&q.body, out);
}

impl Render<'_> {
    fn off(&self, i: &Ident) -> Option<usize> {
        self.pos.offset(i.span.start)
    }

    fn fail(&mut self, why: &str) -> ControlFlow<()> {
        self.err = Some(invalid(why));
        ControlFlow::Break(())
    }

    fn shadowed(&self, name: &str) -> bool {
        let n = name.to_ascii_lowercase();
        self.scopes.iter().any(|s| s.contains(&n))
    }

    /// Replaces the qualifier tokens from `first` up to (not including)
    /// the column ident `col` with the rendered qualifier.
    fn requalify(&mut self, first: &Ident, col: &Ident) -> ControlFlow<()> {
        let (Some(a), Some(b)) = (self.off(first), self.off(col)) else {
            return self.fail("a column position is unknown");
        };
        self.edits.push(Edit {
            start: a,
            end: b,
            with: format!("{}.", self.qual),
        });
        ControlFlow::Continue(())
    }

    fn call(&mut self, name_ident: &Ident, func: &str) -> ControlFlow<()> {
        let Some(start) = self.off(name_ident) else {
            return self.fail("a call position is unknown");
        };
        let Some(i) = self.toks.at(start) else {
            return self.fail("a call position is unknown");
        };
        // The key of SPRIG_CLAIM('k') and SPRIG_USER_ATTR('k'), from the
        // original text (the parse view hides string contents).
        let arg = if KEYED.contains(&func) {
            let t = self.toks.toks.get(i + 2).copied();
            match t {
                Some(t) if t.kind == TokenKind::String && self.toks.punct(i + 3, ")") => {
                    lex::string_value(t.bytes(self.toks.src), self.nbe)
                        .map(|v| String::from_utf8_lossy(&v).into_owned())
                }
                _ => None,
            }
        } else {
            None
        };
        if KEYED.contains(&func) && arg.is_none() {
            return self.fail("a session function's key must be one string");
        }
        let end_tok = if self.toks.punct(i + 1, "(") {
            match self.toks.matching(i + 1) {
                Some(m) => m,
                None => return self.fail("unbalanced call"),
            }
        } else {
            i
        };
        if MEMBERSHIP.contains(&func) {
            let (pre, post) = match self.binder.membership(func) {
                Ok(x) => x,
                Err(e) => {
                    self.err = Some(e);
                    return ControlFlow::Break(());
                }
            };
            self.edits.push(Edit {
                start,
                end: self.toks.end(i + 1),
                with: pre,
            });
            self.edits.push(Edit {
                start: self.toks.start(end_tok),
                end: self.toks.end(end_tok),
                with: post,
            });
            return ControlFlow::Continue(());
        }
        match self.binder.value(func, arg.as_deref()) {
            Ok(v) => {
                self.edits.push(Edit {
                    start,
                    end: self.toks.end(end_tok),
                    with: v,
                });
                ControlFlow::Continue(())
            }
            Err(e) => {
                self.err = Some(e);
                ControlFlow::Break(())
            }
        }
    }
}

impl Visitor for Render<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        let mut names = HashSet::new();
        exposed(q, &mut names);
        self.scopes.push(names);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        let top = self.scopes.is_empty();
        match e {
            Expr::Identifier(i) => {
                if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("CURRENT_USER") {
                    return self.call(i, "CURRENT_USER");
                }
                if top {
                    let Some(a) = self.off(i) else {
                        return self.fail("a column position is unknown");
                    };
                    self.edits.push(Edit {
                        start: a,
                        end: a,
                        with: format!("{}.", self.qual),
                    });
                }
            }
            Expr::CompoundIdentifier(ids) => {
                // table.c, db.table.c
                let (q, col) = match ids.as_slice() {
                    [t, c] => (vec![t], c),
                    [d, t, c] => (vec![d, t], c),
                    _ => return self.fail("a column name has too many parts"),
                };
                let tname = q.last().map(|t| t.value.as_str()).unwrap_or_default();
                let mine = tname.eq_ignore_ascii_case(self.table);
                if top {
                    if !mine {
                        return self.fail("a policy's columns belong to its table");
                    }
                    return self.requalify(q[0], col);
                }
                if mine && !self.shadowed(tname) {
                    return self.requalify(q[0], col);
                }
            }
            Expr::Function(f) => {
                if let Some(name) = fn_name(&f.name)
                    && SESSION_FUNCTIONS.contains(&name.as_str())
                {
                    let Some(ObjectNamePart::Identifier(id)) = f.name.0.first() else {
                        return self.fail("a call position is unknown");
                    };
                    let id = id.clone();
                    return self.call(&id, &name);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Renders a valid policy expression for one reference to `table` (see
/// the module docs). `qual` is the SQL text that qualifies the table's
/// columns there (`` `o` ``, `` `shop`.`orders` ``, `NEW`).
///
/// # Errors
/// 39364 if the expression doesn't parse or a function can't be bound.
pub fn render(
    text: &str,
    lex_opts: &LexOptions,
    table: &str,
    qual: &str,
    binder: &dyn Binder,
) -> Result<String, SprigError> {
    let (view, e) = parse_expr_view(text, *lex_opts)?;
    let all = lex::lex(text.as_bytes(), lex_opts).map_err(|_| invalid("too long"))?;
    let toks = Toks::new(text.as_bytes(), &all);
    // Positions are taken on the view, whose lines and characters are the
    // original's; offsets are the original's.
    let _ = view;
    let pos = Positions::new(text);
    let mut r = Render {
        pos: &pos,
        toks: &toks,
        table,
        qual,
        binder,
        nbe: lex_opts.no_backslash_escapes,
        edits: Vec::new(),
        scopes: Vec::new(),
        err: None,
    };
    let _ = e.visit(&mut r);
    if let Some(err) = r.err {
        return Err(err);
    }
    apply(text, &r.edits).map_err(|_| invalid("overlapping edits"))
}

/// Binds every session function to a literal: the gateway's rewrite.
pub struct LiteralBinder<'a> {
    /// `CURRENT_USER()`: `name@host`.
    pub current_user: &'a str,
    /// `SPRIG_USER_ID()`.
    pub user_id: u64,
    /// `SPRIG_TENANT_ID()`.
    pub tenant_id: u64,
    /// `SPRIG_HOME_TENANT_ID()`.
    pub home_tenant_id: u64,
    /// `SPRIG_SESSION_SPACE()`.
    pub space: Option<u64>,
    /// The session's claims: `SPRIG_CLAIM(k)` as SQL text (`NULL` when
    /// missing).
    pub claim: &'a dyn Fn(&str) -> Option<String>,
    /// The user's attributes, likewise.
    pub attr: &'a dyn Fn(&str) -> Option<String>,
    /// Tenants in the home tenant's subtree, if known.
    pub subtree: Option<&'a [u64]>,
    /// Tenants the user administers, if known.
    pub managed: Option<&'a [u64]>,
    /// The subtree is read from `__sprig_sys.__sprig_subtree_v` instead of
    /// [`Self::subtree`] (above `rls.inline_subtree_max`).
    pub subtree_table: bool,
    /// Likewise for [`Self::managed`] (an administrator's managed tenants
    /// are its subtree).
    pub managed_table: bool,
    /// `NO_BACKSLASH_ESCAPES`.
    pub no_backslash_escapes: bool,
}

fn id_list(ids: &[u64]) -> String {
    if ids.is_empty() {
        return "NULL".to_owned();
    }
    ids.iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Binder for LiteralBinder<'_> {
    fn value(&self, func: &str, arg: Option<&str>) -> Result<String, SprigError> {
        Ok(match func {
            "CURRENT_USER" => super::text::sql_string(self.current_user, self.no_backslash_escapes),
            "SPRIG_USER_ID" => self.user_id.to_string(),
            "SPRIG_TENANT_ID" => self.tenant_id.to_string(),
            "SPRIG_HOME_TENANT_ID" => self.home_tenant_id.to_string(),
            "SPRIG_SESSION_SPACE" => self
                .space
                .map_or_else(|| "NULL".to_owned(), |s| s.to_string()),
            "SPRIG_CLAIM" => (self.claim)(arg.unwrap_or_default()).unwrap_or_else(|| "NULL".into()),
            "SPRIG_USER_ATTR" => {
                (self.attr)(arg.unwrap_or_default()).unwrap_or_else(|| "NULL".into())
            }
            other => return Err(invalid(format!("{other}() can't be bound here"))),
        })
    }

    fn membership(&self, func: &str) -> Result<(String, String), SprigError> {
        let (ids, table) = match func {
            "SPRIG_TENANT_IN_SUBTREE" => (self.subtree, self.subtree_table),
            _ => (self.managed, self.managed_table),
        };
        if table {
            return Ok((
                "(".to_owned(),
                format!(" IN ({}))", super::subtree::subquery()),
            ));
        }
        let ids =
            ids.ok_or_else(|| invalid(format!("{func}() isn't available on this gateway yet")))?;
        Ok(("(".to_owned(), format!(" IN ({}))", id_list(ids))))
    }
}

/// Parses `text` as one expression the way [`render`] reads it (for the
/// statement rewrite's verification).
pub(crate) fn parse_rendered(text: &str) -> Option<Expr> {
    let mut p = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(text)
        .ok()?;
    let e = p.parse_expr().ok()?;
    (p.peek_token().token == SqlToken::EOF).then_some(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lexo() -> LexOptions {
        LexOptions::mariadb()
    }

    struct B;

    impl Binder for B {
        fn value(&self, func: &str, arg: Option<&str>) -> Result<String, SprigError> {
            Ok(match (func, arg) {
                ("SPRIG_CLAIM", Some(k)) => format!("'claim:{k}'"),
                ("CURRENT_USER", _) => "'bob@%'".into(),
                ("SPRIG_USER_ID", _) => "7".into(),
                _ => "NULL".into(),
            })
        }

        fn membership(&self, _func: &str) -> Result<(String, String), SprigError> {
            Ok(("(".into(), " IN (77, 78))".into()))
        }
    }

    #[test]
    fn validation_refuses_what_chapter_29_says() {
        let ok = |s: &str| validate(s, &lexo()).unwrap();
        let bad = |s: &str| {
            let e = validate(s, &lexo()).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::RlsPolicyInvalid), "{s}");
        };
        let i = ok("owner_id = SPRIG_CLAIM('sub')");
        assert_eq!(i.functions, vec!["SPRIG_CLAIM"]);
        let i = ok("team_id IN (SELECT team_id FROM shop.members WHERE uid = SPRIG_CLAIM('sub'))");
        assert_eq!(
            i.tables,
            vec![TableName {
                db: Some("shop".into()),
                name: "members".into()
            }]
        );
        ok("SPRIG_TENANT_MANAGED(org_tenant) AND LOWER(x) LIKE 'a%'");
        ok("owner = CURRENT_USER() OR owner = CURRENT_USER");
        ok("JSON_VALUE(doc, '$.o') = SPRIG_USER_ATTR('org')");
        bad("created_at > NOW()");
        bad("RAND() > 0.5");
        bad("x = @v");
        bad("x = ?");
        bad("x = @@version");
        bad("x IN (SELECT a FROM __sprig_dtx)");
        bad("x IN (SELECT a FROM information_schema.TABLES)");
        bad("shop.f(x) = 1");
        bad("myfunc(x) = 1");
        bad("x = 1 garbage garbage");
        bad("SPRIG_CLAIM(col) = 1");
        bad("SPRIG_TENANT_MANAGED() ");
        bad("x IN (SELECT a FROM t FOR UPDATE)");
        bad("x = CURRENT_TIMESTAMP");
    }

    #[test]
    fn rendering_qualifies_and_binds() {
        let r = |s: &str, q: &str| render(s, &lexo(), "orders", q, &B).unwrap();
        assert_eq!(
            r("owner_id = SPRIG_CLAIM('sub')", "`o`"),
            "`o`.owner_id = 'claim:sub'"
        );
        assert_eq!(
            r("orders.owner = CURRENT_USER() AND `x` > 1", "NEW"),
            "NEW.owner = 'bob@%' AND NEW.`x` > 1"
        );
        assert_eq!(
            r(
                "team_id IN (SELECT team_id FROM members m WHERE m.uid = SPRIG_USER_ID() \
                 AND m.org = orders.org)",
                "`shop`.`orders`"
            ),
            "`shop`.`orders`.team_id IN (SELECT team_id FROM members m WHERE m.uid = 7 \
             AND m.org = `shop`.`orders`.org)"
        );
        // A subquery over the same table: its own `orders` shadows.
        assert_eq!(
            r("EXISTS (SELECT 1 FROM orders WHERE orders.id = 1)", "`o`"),
            "EXISTS (SELECT 1 FROM orders WHERE orders.id = 1)"
        );
        assert_eq!(
            r("SPRIG_TENANT_MANAGED(org_tenant)", "`o`"),
            "(`o`.org_tenant IN (77, 78))"
        );
        assert_eq!(
            r("a = 'it''s' -- x\n AND b = 2", "`t`"),
            "`t`.a = 'it''s' -- x\n AND `t`.b = 2"
        );
        let e = render("other.c = 1", &lexo(), "orders", "`o`", &B).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::RlsPolicyInvalid));
    }

    #[test]
    fn subquery_tables_get_the_policy_database() {
        assert_eq!(
            qualify_tables(
                "t IN (SELECT t FROM members m JOIN other.x ON 1 WHERE m.u = 1)",
                &lexo(),
                "shop"
            )
            .unwrap(),
            "t IN (SELECT t FROM `shop`.members m JOIN other.x ON 1 WHERE m.u = 1)"
        );
    }

    #[test]
    fn schemas_replace_databases() {
        let f = |d: Option<&str>, _t: &str| Some(format!("s_{}", d.unwrap_or("here")));
        assert_eq!(
            map_table_schemas(
                "JSON_VALUE(@sprig_rls, '$.p') AND x IN (SELECT a FROM `shop`.m JOIN n ON 1)",
                &lexo(),
                &f
            )
            .unwrap(),
            "JSON_VALUE(@sprig_rls, '$.p') AND x IN (SELECT a FROM `s_shop`.m JOIN `s_here`.n ON 1)"
        );
    }

    #[test]
    fn literal_binder_quotes_values() {
        let claim = |k: &str| (k == "sub").then(|| super::super::text::sql_string("u'1", false));
        let none = |_: &str| None;
        let b = LiteralBinder {
            current_user: "alice@%",
            user_id: 7,
            tenant_id: 42,
            home_tenant_id: 42,
            space: None,
            claim: &claim,
            attr: &none,
            subtree: Some(&[42, 43]),
            managed: None,
            subtree_table: false,
            managed_table: false,
            no_backslash_escapes: false,
        };
        let r = |s: &str| render(s, &lexo(), "t", "`t`", &b);
        assert_eq!(
            r("o = SPRIG_CLAIM('sub') AND p = SPRIG_CLAIM('x')").unwrap(),
            "`t`.o = 'u''1' AND `t`.p = NULL"
        );
        assert_eq!(
            r("SPRIG_TENANT_IN_SUBTREE(org) AND u = CURRENT_USER").unwrap(),
            "(`t`.org IN (42, 43)) AND `t`.u = 'alice@%'"
        );
        assert!(r("SPRIG_TENANT_MANAGED(org)").is_err());
        // Above rls.inline_subtree_max, the subtree table.
        let t = LiteralBinder {
            subtree: None,
            subtree_table: true,
            managed: Some(&[]),
            ..b
        };
        assert_eq!(
            render(
                "SPRIG_TENANT_IN_SUBTREE(org) OR SPRIG_TENANT_MANAGED(org)",
                &lexo(),
                "t",
                "`t`",
                &t
            )
            .unwrap(),
            "(`t`.org IN (SELECT `descendant` FROM `__sprig_sys`.`__sprig_subtree_v`)) OR (`t`.org IN (NULL))"
        );
    }
}
