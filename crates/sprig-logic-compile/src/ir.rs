//! The shared IR: what every front end lowers to,
//! what the checks and placement read, what the PL/SQL generator emits
//! from, and what the reference interpreter ([`crate::interp`]) runs.
//!
//! As built, the IR is a typed, structured program over scalar and JSON
//! values:
//!
//! - every value may be `NULL`, so there is no `Nullable` type; the types
//!   say what a non-null value is;
//! - loops are one form, [`Stmt::While`], with a [`Bound`]; front ends
//!   lower counted and for-each loops to it with explicit counters;
//! - data access is a statement, [`Stmt::Db`], whose results land in
//!   typed locals (scalarized rows), never an expression.
//!
//! The semantics every consumer must agree on are written on each
//! variant. Where they differ from JavaScript or KIRun,
//! `EXPLAIN FUNCTION` lists the difference.

use std::fmt;

/// A local variable's index into [`Function::locals`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LocalId(pub u32);

/// A value's type. Every type also holds `NULL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ty {
    /// `true` or `false` (`BOOLEAN` in MariaDB).
    Bool,
    /// A whole number, `i64` (`BIGINT`). Overflow is a runtime error.
    Int,
    /// A double (`DOUBLE`). NaN and infinities are runtime errors.
    Float,
    /// Text (`LONGTEXT`, compared by code point).
    Str,
    /// A JSON document: object, array or scalar (`LONGTEXT` holding JSON).
    Json,
    /// Only `NULL` (the type of the `null` literal).
    Null,
}

impl Ty {
    /// Whether the type is `Int` or `Float`.
    #[must_use]
    pub const fn is_number(self) -> bool {
        matches!(self, Ty::Int | Ty::Float)
    }

    /// The common type of two branches or operands, if there is one:
    /// `Null` joins anything, `Int` and `Float` join as `Float`.
    #[must_use]
    pub fn join(self, other: Ty) -> Option<Ty> {
        match (self, other) {
            (a, b) if a == b => Some(a),
            (Ty::Null, b) => Some(b),
            (a, Ty::Null) => Some(a),
            (Ty::Int, Ty::Float) | (Ty::Float, Ty::Int) => Some(Ty::Float),
            _ => None,
        }
    }

    /// The name used in printed IR.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Ty::Bool => "bool",
            Ty::Int => "i64",
            Ty::Float => "f64",
            Ty::Str => "str",
            Ty::Json => "json",
            Ty::Null => "null",
        }
    }
}

impl fmt::Display for Ty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A constant.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A whole number.
    Int(i64),
    /// A double.
    Float(f64),
    /// Text.
    Str(String),
    /// A JSON document (objects and arrays given as values).
    Json(serde_json::Value),
}

impl Literal {
    /// The literal's type.
    #[must_use]
    pub fn ty(&self) -> Ty {
        match self {
            Literal::Null => Ty::Null,
            Literal::Bool(_) => Ty::Bool,
            Literal::Int(_) => Ty::Int,
            Literal::Float(_) => Ty::Float,
            Literal::Str(_) => Ty::Str,
            Literal::Json(_) => Ty::Json,
        }
    }
}

/// One step of a JSON path.
#[derive(Debug, Clone, PartialEq)]
pub enum PathSeg {
    /// An object member.
    Key(String),
    /// An array element; the index is an `Int` expression.
    Index(Box<Expr>),
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    /// Numeric negation; `NULL` stays `NULL`.
    Neg,
    /// Logical not of the operand's truthiness ([`Builtin::Truthy`]);
    /// never `NULL`.
    Not,
}

/// Binary operators. Arithmetic and comparisons return `NULL` when an
/// operand is `NULL`; the logical operators and `Eq`/`Ne` never do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    /// `Int + Int` (checked) or `Float + Float`.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division, always `Float`; dividing by zero is a runtime error.
    Div,
    /// `floor(a / b)` as `Int`; dividing by zero is a runtime error.
    IntDiv,
    /// Remainder with the dividend's sign; by zero is a runtime error.
    Mod,
    /// Text concatenation of two `Str`; a `NULL` operand reads as `null`.
    Concat,
    /// Null-safe equality (`<=>`): `NULL` equals only `NULL`; numbers
    /// compare by value, text by code point.
    Eq,
    /// The negation of `Eq`.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
    /// Both operands truthy ([`Builtin::Truthy`]).
    And,
    /// Either operand truthy.
    Or,
    /// The left operand unless it is `NULL`, else the right.
    Coalesce,
}

impl BinOp {
    /// The name used in printed IR.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            BinOp::Add => "add",
            BinOp::Sub => "sub",
            BinOp::Mul => "mul",
            BinOp::Div => "div",
            BinOp::IntDiv => "idiv",
            BinOp::Mod => "mod",
            BinOp::Concat => "concat",
            BinOp::Eq => "seq",
            BinOp::Ne => "sne",
            BinOp::Lt => "lt",
            BinOp::Le => "le",
            BinOp::Gt => "gt",
            BinOp::Ge => "ge",
            BinOp::And => "and",
            BinOp::Or => "or",
            BinOp::Coalesce => "coalesce",
        }
    }

    /// Whether the operator compares (returns `Bool`).
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
        )
    }
}

/// Built-in functions. `NULL` in gives `NULL` out unless a variant says
/// otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builtin {
    /// JavaScript-style truthiness, never `NULL`: `NULL` and `false` are
    /// false, numbers are true unless zero, text and JSON are true (even
    /// `''`, as KIRun's `If` reads it).
    Truthy,
    /// KIRun's `executeIftrue` test, never `NULL`: true unless `NULL` or
    /// `false`.
    Present,
    /// `Int` or `Float` to `Float`.
    ToFloat,
    /// Any scalar to text: integers in decimal, doubles without `.0` when
    /// whole and below 2^53, booleans as `true`/`false`, `NULL` as `null`.
    ToStr,
    /// The number in a JSON value: a JSON number, text holding one (other
    /// text and booleans read as 0, as MariaDB casts them); `NULL` for
    /// objects and arrays.
    JsonNum,
    /// The text in a JSON value: a JSON string unquoted, a number's text,
    /// `1` or `0` for booleans (MariaDB's `JSON_VALUE`); `NULL` for
    /// objects and arrays.
    JsonStr,
    /// A scalar as a JSON value.
    ToJson,
    /// The number of elements of a JSON array or members of an object.
    JsonLength,
    /// Absolute value.
    Abs,
    /// Round down, `Float` to `Float`.
    Floor,
    /// Round up.
    Ceil,
    /// Round half up (`floor(x + 0.5)`), as KIRun and JavaScript do.
    Round,
    /// Square root; `NULL` for negative numbers.
    Sqrt,
    /// `a` to the power `b`.
    Pow,
    /// The largest argument; `NULL` if any is `NULL`.
    Max,
    /// The smallest argument; `NULL` if any is `NULL`.
    Min,
    /// Length in code points.
    Len,
    /// Upper case.
    Upper,
    /// Lower case.
    Lower,
    /// Whether `a` contains `b`.
    Contains,
    /// Whether `a` starts with `b`.
    StartsWith,
    /// Whether `a` ends with `b`.
    EndsWith,
    /// The code-point index of `b` in `a`, or -1.
    IndexOf,
    /// Its argument; a runtime error (39866) when it is `NULL`. KIRun's
    /// arithmetic operators refuse a null operand, so the KIRun front end
    /// wraps every operand that may be `NULL`.
    NotNull,
}

impl Builtin {
    /// The name used in printed IR.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Builtin::Truthy => "truthy",
            Builtin::Present => "present",
            Builtin::ToFloat => "to_f64",
            Builtin::ToStr => "num_str",
            Builtin::JsonNum => "json_num",
            Builtin::JsonStr => "json_str",
            Builtin::ToJson => "to_json",
            Builtin::JsonLength => "json_len",
            Builtin::Abs => "abs",
            Builtin::Floor => "floor",
            Builtin::Ceil => "ceil",
            Builtin::Round => "round",
            Builtin::Sqrt => "sqrt",
            Builtin::Pow => "pow",
            Builtin::Max => "max",
            Builtin::Min => "min",
            Builtin::Len => "len",
            Builtin::Upper => "upper",
            Builtin::Lower => "lower",
            Builtin::Contains => "contains",
            Builtin::StartsWith => "starts_with",
            Builtin::EndsWith => "ends_with",
            Builtin::IndexOf => "index_of",
            Builtin::NotNull => "not_null",
        }
    }
}

/// An expression. Every expression has one type, given by
/// [`Function::ty_of`].
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A constant.
    Lit(Literal),
    /// A local variable.
    Local(LocalId),
    /// A parameter, by position.
    Param(u16),
    /// A member or element of a `Json` value; `NULL` when missing or JSON
    /// `null`. The result is `Json`.
    Path(Box<Expr>, Vec<PathSeg>),
    /// A unary operator.
    Unary(UnOp, Box<Expr>),
    /// A binary operator.
    Binary(BinOp, Box<Expr>, Box<Expr>),
    /// `if c then a else b`, by `c`'s truthiness; `c` is `Bool`.
    Cond(Box<Expr>, Box<Expr>, Box<Expr>),
    /// A built-in function.
    Builtin(Builtin, Vec<Expr>),
    /// A JSON object built from members.
    ObjectLit(Vec<(String, Expr)>),
    /// A JSON array built from elements.
    ArrayLit(Vec<Expr>),
}

impl Expr {
    /// A literal integer.
    #[must_use]
    pub fn int(v: i64) -> Self {
        Expr::Lit(Literal::Int(v))
    }

    /// A literal boolean.
    #[must_use]
    pub fn bool(v: bool) -> Self {
        Expr::Lit(Literal::Bool(v))
    }

    /// A binary expression.
    #[must_use]
    pub fn bin(op: BinOp, a: Expr, b: Expr) -> Self {
        Expr::Binary(op, Box::new(a), Box::new(b))
    }

    /// A built-in call.
    #[must_use]
    pub fn call(b: Builtin, args: Vec<Expr>) -> Self {
        Expr::Builtin(b, args)
    }

    /// Whether this is the literal `true`.
    #[must_use]
    pub fn is_true(&self) -> bool {
        matches!(self, Expr::Lit(Literal::Bool(true)))
    }

    /// Calls `f` on this expression and every expression inside it.
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        match self {
            Expr::Lit(_) | Expr::Local(_) | Expr::Param(_) => {}
            Expr::Path(e, segs) => {
                e.walk(f);
                for s in segs {
                    if let PathSeg::Index(i) = s {
                        i.walk(f);
                    }
                }
            }
            Expr::Unary(_, e) => e.walk(f),
            Expr::Binary(_, a, b) => {
                a.walk(f);
                b.walk(f);
            }
            Expr::Cond(c, a, b) => {
                c.walk(f);
                a.walk(f);
                b.walk(f);
            }
            Expr::Builtin(_, args) | Expr::ArrayLit(args) => {
                for a in args {
                    a.walk(f);
                }
            }
            Expr::ObjectLit(m) => {
                for (_, v) in m {
                    v.walk(f);
                }
            }
        }
    }
}

/// How a loop is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    /// At most this many iterations, known at compile time.
    Static(u64),
    /// Unknown: the generated code counts iterations and fails with 39864
    /// past `logic.max_loop_iterations`.
    Guarded,
}

/// A runtime error the code raises itself.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorSpec {
    /// The SprigDB error number: 39863, 39864, 39865 or 39866.
    pub code: u16,
    /// The message, `Str`; cut to 128 characters.
    pub message: Expr,
}

/// What an embedded SQL statement returns.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlShape {
    /// At most one row, scalarized: `found` is set to whether a row came
    /// back, each column lands in its local (`NULL` when none did), and
    /// more than one row is a runtime error.
    One {
        /// Set to whether a row came back.
        found: LocalId,
        /// Column name and the local it lands in, in select-list order.
        into: Vec<(String, LocalId)>,
    },
    /// Every row, as a JSON array of objects, in result order.
    Many {
        /// The `Json` local the array lands in.
        into: LocalId,
        /// Column names and types, in select-list order.
        cols: Vec<(String, Ty)>,
    },
    /// DML: the number of rows changed (MariaDB's `ROW_COUNT()`, changed
    /// rows, not matched rows).
    Exec {
        /// The `Int` local the count lands in.
        affected: LocalId,
    },
}

/// One embedded SQL statement, bound against the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundSql {
    /// The statement re-rendered from its AST, split at its bind
    /// variables: `parts.len() == binds.len() + 1`.
    pub parts: Vec<String>,
    /// Tables it reads.
    pub reads: Vec<String>,
    /// Tables it writes.
    pub writes: Vec<String>,
}

impl BoundSql {
    /// The text with `?` for each bind variable.
    #[must_use]
    pub fn with_placeholders(&self) -> String {
        self.parts.join("?")
    }
}

/// A data operation.
#[derive(Debug, Clone, PartialEq)]
pub enum DbOp {
    /// SQL with bind values.
    Sql {
        /// The statement.
        stmt: BoundSql,
        /// One expression per bind variable.
        binds: Vec<Expr>,
        /// What it returns, and where.
        shape: SqlShape,
        /// Where it came from, for messages (`step item`).
        origin: String,
    },
}

/// A block of statements.
pub type Block = Vec<Stmt>;

/// A statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    /// Assigns a local (its first assignment declares it in printed IR).
    Assign(LocalId, Expr),
    /// `if c { a } else { b }`; `c` is `Bool`, `NULL` reads as false.
    If(Expr, Block, Block),
    /// `while c { body }`, with a bound.
    While {
        /// Checked before each iteration, `Bool`.
        cond: Expr,
        /// The body.
        body: Block,
        /// The bound.
        bound: Bound,
    },
    /// Leaves the innermost loop.
    Break,
    /// Goes on with the innermost loop's next iteration (its condition
    /// is checked again). Front ends put a counted loop's update before
    /// it.
    Continue,
    /// Raises an event: the first event raised is the call's result
    /// (later ones are ignored), and raising `output` ends the call.
    Emit(String, Vec<(String, Expr)>),
    /// Fails the call.
    Throw(ErrorSpec),
    /// Ends the call.
    Return,
    /// A data operation.
    Db(DbOp),
}

/// A parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    /// The name in the source.
    pub name: String,
    /// Its type.
    pub ty: Ty,
    /// The smallest value the signature allows (KIRun `minimum`).
    pub min: Option<f64>,
    /// The largest value the signature allows (KIRun `maximum`).
    pub max: Option<f64>,
}

/// An event the function can raise.
#[derive(Debug, Clone, PartialEq)]
pub struct EventDef {
    /// The event's name, such as `output` or `error`.
    pub name: String,
    /// Its fields and their types.
    pub fields: Vec<(String, Ty)>,
}

/// A local variable.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalDef {
    /// Its name, unique in the function (`item_price`).
    pub name: String,
    /// Its type.
    pub ty: Ty,
}

/// A function in IR.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Function {
    /// The source's name, such as `Shop.Quote`.
    pub name: String,
    /// Parameters, in call order.
    pub params: Vec<Param>,
    /// Events, `output` first.
    pub events: Vec<EventDef>,
    /// Locals; every local starts as `NULL`.
    pub locals: Vec<LocalDef>,
    /// The body.
    pub body: Block,
}

impl Function {
    /// Adds a local with a name made unique, and returns it.
    pub fn local(&mut self, name: &str, ty: Ty) -> LocalId {
        let base = sanitize(name);
        let mut n = base.clone();
        let mut i = 2;
        while self.locals.iter().any(|l| l.name == n) {
            n = format!("{base}_{i}");
            i += 1;
        }
        self.locals.push(LocalDef { name: n, ty });
        LocalId(u32::try_from(self.locals.len() - 1).unwrap_or(u32::MAX))
    }

    /// A local's definition.
    ///
    /// # Panics
    /// Never for ids this function made.
    #[must_use]
    pub fn local_def(&self, id: LocalId) -> &LocalDef {
        &self.locals[id.0 as usize]
    }

    /// The event called `name`.
    #[must_use]
    pub fn event(&self, name: &str) -> Option<&EventDef> {
        self.events.iter().find(|e| e.name == name)
    }

    /// The type of `e`.
    #[must_use]
    pub fn ty_of(&self, e: &Expr) -> Ty {
        match e {
            Expr::Lit(l) => l.ty(),
            Expr::Local(id) => self.local_def(*id).ty,
            Expr::Param(i) => self.params.get(*i as usize).map_or(Ty::Null, |p| p.ty),
            Expr::Path(..) | Expr::ObjectLit(_) | Expr::ArrayLit(_) => Ty::Json,
            Expr::Unary(UnOp::Neg, a) => self.ty_of(a),
            Expr::Unary(UnOp::Not, _) => Ty::Bool,
            Expr::Binary(op, a, b) => match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod => {
                    if self.ty_of(a) == Ty::Int && self.ty_of(b) == Ty::Int {
                        Ty::Int
                    } else {
                        Ty::Float
                    }
                }
                BinOp::Div => Ty::Float,
                BinOp::IntDiv => Ty::Int,
                BinOp::Concat => Ty::Str,
                BinOp::Coalesce => self.ty_of(a).join(self.ty_of(b)).unwrap_or(Ty::Json),
                _ => Ty::Bool,
            },
            Expr::Cond(_, a, b) => self.ty_of(a).join(self.ty_of(b)).unwrap_or(Ty::Json),
            Expr::Builtin(b, args) => match b {
                Builtin::Truthy
                | Builtin::Present
                | Builtin::Contains
                | Builtin::StartsWith
                | Builtin::EndsWith => Ty::Bool,
                Builtin::ToFloat
                | Builtin::JsonNum
                | Builtin::Floor
                | Builtin::Ceil
                | Builtin::Round
                | Builtin::Sqrt
                | Builtin::Pow => Ty::Float,
                Builtin::ToStr | Builtin::JsonStr | Builtin::Upper | Builtin::Lower => Ty::Str,
                Builtin::ToJson => Ty::Json,
                Builtin::JsonLength | Builtin::Len | Builtin::IndexOf => Ty::Int,
                Builtin::Abs | Builtin::NotNull => {
                    args.first().map_or(Ty::Float, |a| self.ty_of(a))
                }
                Builtin::Max | Builtin::Min => args
                    .iter()
                    .map(|a| self.ty_of(a))
                    .try_fold(Ty::Null, Ty::join)
                    .unwrap_or(Ty::Float),
            },
        }
    }

    /// The number of IR nodes (statements and expressions).
    #[must_use]
    pub fn size(&self) -> usize {
        fn block(b: &Block) -> usize {
            b.iter().map(stmt).sum()
        }
        fn expr(e: &Expr) -> usize {
            let mut n = 0;
            e.walk(&mut |_| n += 1);
            n
        }
        fn stmt(s: &Stmt) -> usize {
            1 + match s {
                Stmt::Assign(_, e) => expr(e),
                Stmt::If(c, a, b) => expr(c) + block(a) + block(b),
                Stmt::While { cond, body, .. } => expr(cond) + block(body),
                Stmt::Emit(_, f) => f.iter().map(|(_, e)| expr(e)).sum(),
                Stmt::Throw(e) => expr(&e.message),
                Stmt::Db(DbOp::Sql { binds, .. }) => binds.iter().map(expr).sum(),
                Stmt::Break | Stmt::Continue | Stmt::Return => 0,
            }
        }
        block(&self.body)
    }
}

/// `[A-Za-z0-9_]` only, lower case, not starting with a digit.
#[must_use]
pub fn sanitize(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() || s.starts_with(|c: char| c.is_ascii_digit()) {
        s.insert(0, 'x');
    }
    s
}

// ---------------------------------------------------------------------------
// Printing (`EXPLAIN FUNCTION`, golden tests).

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let params: Vec<String> = self
            .params
            .iter()
            .map(|p| {
                let mut s = format!("{}: {}", p.name, p.ty);
                if p.min.is_some() || p.max.is_some() {
                    let n = |v: Option<f64>| v.map(|x| x.to_string()).unwrap_or_default();
                    s.push_str(&format!(" [{}..{}]", n(p.min), n(p.max)));
                }
                s
            })
            .collect();
        let events: Vec<String> = self
            .events
            .iter()
            .map(|e| {
                let fs: Vec<String> = e.fields.iter().map(|(n, t)| format!("{n}: {t}")).collect();
                format!("{}{{{}}}", e.name, fs.join(", "))
            })
            .collect();
        writeln!(
            f,
            "function {} ({}) -> events {{{}}}",
            self.name,
            params.join(", "),
            events.join(", ")
        )?;
        let mut p = Printer {
            f: self,
            out: String::new(),
            line: 0,
        };
        p.block(&self.body, 1);
        f.write_str(&p.out)
    }
}

struct Printer<'a> {
    f: &'a Function,
    out: String,
    line: usize,
}

impl Printer<'_> {
    fn line(&mut self, depth: usize, text: &str) {
        self.line += 1;
        self.out.push_str(&format!(
            "{:>3} {}{}\n",
            self.line,
            "  ".repeat(depth.saturating_sub(1)),
            text
        ));
    }

    fn block(&mut self, b: &Block, depth: usize) {
        for s in b {
            self.stmt(s, depth);
        }
    }

    fn stmt(&mut self, s: &Stmt, d: usize) {
        match s {
            Stmt::Assign(id, e) => {
                let l = self.f.local_def(*id);
                let t = format!("{}: {} := {}", l.name, l.ty, self.expr(e));
                self.line(d, &t);
            }
            Stmt::If(c, a, b) => {
                let t = format!("if {}", self.expr(c));
                self.line(d, &t);
                self.block(a, d + 1);
                if !b.is_empty() {
                    self.line(d, "else");
                    self.block(b, d + 1);
                }
            }
            Stmt::While { cond, body, bound } => {
                let bd = match bound {
                    Bound::Static(n) => format!("bound {n}"),
                    Bound::Guarded => "guarded".to_owned(),
                };
                let t = format!("while {} ({bd})", self.expr(cond));
                self.line(d, &t);
                self.block(body, d + 1);
            }
            Stmt::Break => self.line(d, "break"),
            Stmt::Continue => self.line(d, "continue"),
            Stmt::Return => self.line(d, "return"),
            Stmt::Emit(name, fields) => {
                let fs: Vec<String> = fields
                    .iter()
                    .map(|(n, e)| format!("{n}: {}", self.expr(e)))
                    .collect();
                let t = format!("emit {name} {{{}}}", fs.join(", "));
                self.line(d, &t);
            }
            Stmt::Throw(e) => {
                let t = format!("throw({}, {})", e.code, self.expr(&e.message));
                self.line(d, &t);
            }
            Stmt::Db(DbOp::Sql {
                stmt, binds, shape, ..
            }) => {
                let bs: Vec<String> = binds.iter().map(|b| self.expr(b)).collect();
                let call = format!("{:?}, [{}]", stmt.with_placeholders(), bs.join(", "));
                let t = match shape {
                    SqlShape::One { found, into } => {
                        let names: Vec<&str> = std::iter::once(*found)
                            .chain(into.iter().map(|(_, l)| *l))
                            .map(|l| self.f.local_def(l).name.as_str())
                            .collect();
                        format!("({}) := db.sql_one({call})", names.join(", "))
                    }
                    SqlShape::Many { into, .. } => {
                        format!("{} := db.sql({call})", self.f.local_def(*into).name)
                    }
                    SqlShape::Exec { affected } => {
                        format!("{} := db.exec({call})", self.f.local_def(*affected).name)
                    }
                };
                self.line(d, &t);
            }
        }
    }

    fn expr(&self, e: &Expr) -> String {
        match e {
            Expr::Lit(l) => match l {
                Literal::Null => "null".to_owned(),
                Literal::Bool(b) => b.to_string(),
                Literal::Int(i) => i.to_string(),
                Literal::Float(x) => format!("{x:?}"),
                Literal::Str(s) => format!("{s:?}"),
                Literal::Json(v) => format!("json({v})"),
            },
            Expr::Local(id) => self.f.local_def(*id).name.clone(),
            Expr::Param(i) => self
                .f
                .params
                .get(*i as usize)
                .map_or_else(|| format!("p{i}"), |p| p.name.clone()),
            Expr::Path(b, segs) => {
                let mut s = self.expr(b);
                for seg in segs {
                    match seg {
                        PathSeg::Key(k) => s.push_str(&format!(".{k}")),
                        PathSeg::Index(i) => s.push_str(&format!("[{}]", self.expr(i))),
                    }
                }
                s
            }
            Expr::Unary(UnOp::Neg, a) => format!("neg({})", self.expr(a)),
            Expr::Unary(UnOp::Not, a) => format!("not({})", self.expr(a)),
            Expr::Binary(op, a, b) => format!("{}({}, {})", op.name(), self.expr(a), self.expr(b)),
            Expr::Cond(c, a, b) => {
                format!("cond({}, {}, {})", self.expr(c), self.expr(a), self.expr(b))
            }
            Expr::Builtin(b, args) => {
                let a: Vec<String> = args.iter().map(|x| self.expr(x)).collect();
                format!("{}({})", b.name(), a.join(", "))
            }
            Expr::ObjectLit(m) => {
                let a: Vec<String> = m
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", self.expr(v)))
                    .collect();
                format!("{{{}}}", a.join(", "))
            }
            Expr::ArrayLit(v) => {
                let a: Vec<String> = v.iter().map(|x| self.expr(x)).collect();
                format!("[{}]", a.join(", "))
            }
        }
    }
}
