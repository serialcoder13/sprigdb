//! A reference interpreter of the IR: the meaning the PL/SQL generator
//! must reproduce, run directly in Rust. The differential suite runs the
//! same functions here and as compiled routines in MariaDB and compares
//! events, results, errors and table contents.
//!
//! Data access goes through [`Db`], statement by statement, with the
//! statement text of [`crate::ir::BoundSql::with_placeholders`] and its
//! bind values: the way the engine tier will run embedded SQL.
//!
//! Errors are SprigDB errors: 39864 (loop budget), 39865 (thrown), 39866
//! (runtime: overflow, division by zero, `QueryOne` with many rows, no
//! event). MariaDB raises 1365 and 1690 for the same runtime errors; the
//! gateway maps them to 39866.

use std::cmp::Ordering as O;

use serde_json::{Map, Number, Value as J};
use sprig_common::{Result, SprigCode, SprigError};

use crate::ir::{
    BinOp, Bound, Builtin, DbOp, Expr, Function, Literal, PathSeg, SqlShape, Stmt, Ty, UnOp,
};

/// A runtime value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
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
    /// A JSON document other than JSON `null`.
    Json(J),
}

impl Value {
    /// The value as JSON (`NULL` as JSON `null`).
    #[must_use]
    pub fn to_json(&self) -> J {
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::Int(i) => J::Number((*i).into()),
            Value::Float(x) => Number::from_f64(*x).map_or(J::Null, J::Number),
            Value::Str(s) => J::String(s.clone()),
            Value::Json(j) => j.clone(),
        }
    }

    /// A JSON value as a runtime value (JSON `null` is `NULL`).
    #[must_use]
    pub fn from_json(j: J) -> Self {
        if j.is_null() {
            Value::Null
        } else {
            Value::Json(j)
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Data access for the interpreter.
pub trait Db {
    /// Runs a query; each row's columns as text (`None` for `NULL`).
    ///
    /// # Errors
    /// The server's error.
    fn query(&mut self, sql: &str, binds: &[Value]) -> Result<Vec<Vec<Option<String>>>>;

    /// Runs DML; the number of rows changed.
    ///
    /// # Errors
    /// The server's error.
    fn exec(&mut self, sql: &str, binds: &[Value]) -> Result<u64>;
}

/// A `Db` with no tables, for functions without data access.
#[derive(Debug, Default)]
pub struct NoDb;

impl Db for NoDb {
    fn query(&mut self, _: &str, _: &[Value]) -> Result<Vec<Vec<Option<String>>>> {
        Err(SprigError::internal("no database"))
    }
    fn exec(&mut self, _: &str, _: &[Value]) -> Result<u64> {
        Err(SprigError::internal("no database"))
    }
}

/// The call's result: the first event raised, and its fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    /// The event's name.
    pub event: String,
    /// Its fields, as a JSON object.
    pub result: J,
}

/// Limits the interpreter enforces.
#[derive(Debug, Clone)]
pub struct Limits {
    /// `logic.max_loop_iterations`, for guarded loops.
    pub max_loop_iterations: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_loop_iterations: 100_000,
        }
    }
}

fn runtime(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicRuntimeError, m)
}

enum Flow {
    Next,
    Break,
    Continue,
    Return,
}

struct Run<'a> {
    f: &'a Function,
    args: &'a [Value],
    locals: Vec<Value>,
    event: Option<(String, J)>,
    db: &'a mut dyn Db,
    limits: &'a Limits,
}

/// Runs `f` with `args` (one per parameter, already of the parameter's
/// type).
///
/// # Errors
/// 39874 for a wrong number of arguments; otherwise the runtime errors
/// listed in the module docs, or `db`'s errors.
pub fn run(f: &Function, args: &[Value], db: &mut dyn Db, limits: &Limits) -> Result<Outcome> {
    if args.len() != f.params.len() {
        return Err(SprigError::new(
            SprigCode::LogicSignatureMismatch,
            format!(
                "{} takes {} arguments, not {}",
                f.name,
                f.params.len(),
                args.len()
            ),
        ));
    }
    let mut r = Run {
        f,
        args,
        locals: vec![Value::Null; f.locals.len()],
        event: None,
        db,
        limits,
    };
    r.block(&f.body)?;
    if let Some((event, result)) = r.event {
        return Ok(Outcome { event, result });
    }
    if f.event("output").is_some_and(|e| !e.fields.is_empty()) {
        return Err(runtime("No events raised"));
    }
    Ok(Outcome {
        event: "output".to_owned(),
        result: J::Object(Map::new()),
    })
}

impl Run<'_> {
    fn block(&mut self, b: &[Stmt]) -> Result<Flow> {
        for s in b {
            match self.stmt(s)? {
                Flow::Next => {}
                other => return Ok(other),
            }
        }
        Ok(Flow::Next)
    }

    fn stmt(&mut self, s: &Stmt) -> Result<Flow> {
        match s {
            Stmt::Assign(l, e) => {
                let v = self.eval(e)?;
                let ty = self.f.local_def(*l).ty;
                self.locals[l.0 as usize] = conform(v, ty);
            }
            Stmt::If(c, a, b) => {
                let c = self.eval(c)?;
                return if truthy(&c) {
                    self.block(a)
                } else {
                    self.block(b)
                };
            }
            Stmt::While { cond, body, bound } => {
                let mut n: u64 = 0;
                loop {
                    let c = self.eval(cond)?;
                    if !truthy(&c) {
                        break;
                    }
                    n += 1;
                    if *bound == Bound::Guarded && n > self.limits.max_loop_iterations {
                        return Err(SprigError::new(
                            SprigCode::LogicBudgetExceeded,
                            format!(
                                "loop budget: more than {} iterations",
                                self.limits.max_loop_iterations
                            ),
                        ));
                    }
                    match self.block(body)? {
                        Flow::Next | Flow::Continue => {}
                        Flow::Break => break,
                        Flow::Return => return Ok(Flow::Return),
                    }
                }
            }
            Stmt::Break => return Ok(Flow::Break),
            Stmt::Continue => return Ok(Flow::Continue),
            Stmt::Return => return Ok(Flow::Return),
            Stmt::Emit(name, fields) => {
                if self.event.is_none() {
                    let mut m = Map::new();
                    for (k, e) in fields {
                        let v = self.eval(e)?;
                        m.insert(k.clone(), v.to_json());
                    }
                    self.event = Some((name.clone(), J::Object(m)));
                }
                if name == "output" {
                    return Ok(Flow::Return);
                }
            }
            Stmt::Throw(e) => {
                let m = self.eval(&e.message)?;
                let text: String = to_str(&m).chars().take(128).collect();
                let code = match e.code {
                    39864 => SprigCode::LogicBudgetExceeded,
                    39866 => SprigCode::LogicRuntimeError,
                    _ => SprigCode::LogicUserError,
                };
                return Err(SprigError::new(code, text));
            }
            Stmt::Db(DbOp::Sql {
                stmt,
                binds,
                shape,
                origin,
            }) => {
                let sql = stmt.with_placeholders();
                let mut vals = Vec::with_capacity(binds.len());
                for b in binds {
                    vals.push(self.eval(b)?);
                }
                match shape {
                    SqlShape::One { found, into } => {
                        let rows = self.db.query(&sql, &vals)?;
                        if rows.len() > 1 {
                            return Err(runtime(format!(
                                "Sprig.Db.QueryOne at {origin} returned more than one row"
                            )));
                        }
                        self.locals[found.0 as usize] = Value::Bool(rows.len() == 1);
                        for (i, (_, l)) in into.iter().enumerate() {
                            let ty = self.f.local_def(*l).ty;
                            let cell = rows.first().and_then(|r| r.get(i).cloned()).flatten();
                            self.locals[l.0 as usize] = from_text(cell, ty)?;
                        }
                    }
                    SqlShape::Many { into, cols } => {
                        let rows = self.db.query(&sql, &vals)?;
                        let mut arr = Vec::with_capacity(rows.len());
                        for r in rows {
                            let mut m = Map::new();
                            for (i, (c, t)) in cols.iter().enumerate() {
                                let v = from_text(r.get(i).cloned().flatten(), *t)?;
                                m.insert(c.clone(), v.to_json());
                            }
                            arr.push(J::Object(m));
                        }
                        self.locals[into.0 as usize] = Value::Json(J::Array(arr));
                    }
                    SqlShape::Exec { affected } => {
                        let n = self.db.exec(&sql, &vals)?;
                        self.locals[affected.0 as usize] =
                            Value::Int(i64::try_from(n).unwrap_or(i64::MAX));
                    }
                }
            }
        }
        Ok(Flow::Next)
    }

    #[allow(clippy::too_many_lines)]
    fn eval(&mut self, e: &Expr) -> Result<Value> {
        Ok(match e {
            Expr::Lit(l) => match l {
                Literal::Null => Value::Null,
                Literal::Bool(b) => Value::Bool(*b),
                Literal::Int(i) => Value::Int(*i),
                Literal::Float(x) => Value::Float(*x),
                Literal::Str(s) => Value::Str(s.clone()),
                Literal::Json(j) => Value::from_json(j.clone()),
            },
            Expr::Local(l) => self.locals[l.0 as usize].clone(),
            Expr::Param(i) => self.args.get(*i as usize).cloned().unwrap_or(Value::Null),
            Expr::Path(b, segs) => {
                let base = self.eval(b)?;
                let mut cur = match base {
                    Value::Json(j) => j,
                    Value::Str(s) => serde_json::from_str(&s).unwrap_or(J::Null),
                    _ => return Ok(Value::Null),
                };
                for s in segs {
                    cur = match s {
                        PathSeg::Key(k) => match cur {
                            J::Object(mut m) => m.remove(k).unwrap_or(J::Null),
                            _ => J::Null,
                        },
                        PathSeg::Index(i) => {
                            let i = self.eval(i)?;
                            match (cur, i) {
                                (J::Array(mut a), Value::Int(i))
                                    if i >= 0 && (i as usize) < a.len() =>
                                {
                                    a.swap_remove(i as usize)
                                }
                                _ => J::Null,
                            }
                        }
                    };
                }
                Value::from_json(cur)
            }
            Expr::Unary(UnOp::Neg, a) => match self.eval(a)? {
                Value::Int(i) => Value::Int(
                    i.checked_neg()
                        .ok_or_else(|| runtime("BIGINT value is out of range"))?,
                ),
                Value::Float(x) => Value::Float(-x),
                _ => Value::Null,
            },
            Expr::Unary(UnOp::Not, a) => {
                let v = self.eval(a)?;
                Value::Bool(!truthy(&v))
            }
            Expr::Binary(op, a, b) => {
                let (x, y) = (self.eval(a)?, self.eval(b)?);
                binary(*op, x, y)?
            }
            Expr::Cond(c, a, b) => {
                let c = self.eval(c)?;
                let ty = self.f.ty_of(e);
                let v = if truthy(&c) {
                    self.eval(a)?
                } else {
                    self.eval(b)?
                };
                conform(v, ty)
            }
            Expr::Builtin(b, args) => {
                let mut vals = Vec::with_capacity(args.len());
                for a in args {
                    vals.push(self.eval(a)?);
                }
                builtin(*b, &vals)?
            }
            Expr::ObjectLit(m) => {
                let mut o = Map::new();
                for (k, v) in m {
                    let v = self.eval(v)?;
                    o.insert(k.clone(), v.to_json());
                }
                Value::Json(J::Object(o))
            }
            Expr::ArrayLit(v) => {
                let mut a = Vec::with_capacity(v.len());
                for x in v {
                    a.push(self.eval(x)?.to_json());
                }
                Value::Json(J::Array(a))
            }
        })
    }
}

/// A value stored into a local of type `ty` (MariaDB converts on
/// assignment): `Int` into a `Float` local becomes a double.
fn conform(v: Value, ty: Ty) -> Value {
    match (v, ty) {
        (Value::Int(i), Ty::Float) => Value::Float(i as f64),
        (v, _) => v,
    }
}

/// JavaScript-style truthiness (IR `truthy`).
#[must_use]
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(x) => *x != 0.0,
        Value::Str(_) | Value::Json(_) => true,
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(x) => Some(*x),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn finite(x: f64) -> Result<Value> {
    if x.is_finite() {
        Ok(Value::Float(x))
    } else {
        Err(runtime("DOUBLE value is out of range"))
    }
}

fn overflow() -> SprigError {
    runtime("BIGINT value is out of range")
}

fn div_zero() -> SprigError {
    runtime("Division by 0")
}

fn binary(op: BinOp, x: Value, y: Value) -> Result<Value> {
    use Value as V;
    match op {
        BinOp::And => return Ok(V::Bool(truthy(&x) && truthy(&y))),
        BinOp::Or => return Ok(V::Bool(truthy(&x) || truthy(&y))),
        BinOp::Coalesce => return Ok(if x.is_null() { y } else { x }),
        BinOp::Concat => return Ok(V::Str(format!("{}{}", to_str(&x), to_str(&y)))),
        BinOp::Eq | BinOp::Ne => {
            let eq = match (&x, &y) {
                (V::Null, V::Null) => true,
                (V::Null, _) | (_, V::Null) => false,
                (V::Str(a), V::Str(b)) => a == b,
                (V::Json(a), V::Json(b)) => a == b,
                _ => match (num(&x), num(&y)) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                },
            };
            return Ok(V::Bool(if op == BinOp::Eq { eq } else { !eq }));
        }
        _ => {}
    }
    if x.is_null() || y.is_null() {
        return Ok(V::Null);
    }
    if op.is_comparison() {
        let ord = match (&x, &y) {
            (V::Str(a), V::Str(b)) => a.cmp(b),
            _ => match (num(&x), num(&y)) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
                _ => return Ok(V::Null),
            },
        };
        let r = match op {
            BinOp::Lt => ord == O::Less,
            BinOp::Le => ord != O::Greater,
            BinOp::Gt => ord == O::Greater,
            _ => ord != O::Less,
        };
        return Ok(V::Bool(r));
    }
    if let (V::Int(a), V::Int(b)) = (&x, &y) {
        let (a, b) = (*a, *b);
        return match op {
            BinOp::Add => a.checked_add(b).map(V::Int).ok_or_else(overflow),
            BinOp::Sub => a.checked_sub(b).map(V::Int).ok_or_else(overflow),
            BinOp::Mul => a.checked_mul(b).map(V::Int).ok_or_else(overflow),
            BinOp::Mod => {
                if b == 0 {
                    Err(div_zero())
                } else {
                    Ok(V::Int(a.checked_rem(b).unwrap_or(0)))
                }
            }
            BinOp::Div => {
                if b == 0 {
                    Err(div_zero())
                } else {
                    finite(a as f64 / b as f64)
                }
            }
            BinOp::IntDiv => {
                if b == 0 {
                    Err(div_zero())
                } else {
                    #[allow(clippy::cast_possible_truncation)]
                    Ok(V::Int((a as f64 / b as f64).floor() as i64))
                }
            }
            _ => Ok(V::Null),
        };
    }
    let (Some(a), Some(b)) = (num(&x), num(&y)) else {
        return Ok(V::Null);
    };
    match op {
        BinOp::Add => finite(a + b),
        BinOp::Sub => finite(a - b),
        BinOp::Mul => finite(a * b),
        BinOp::Div => {
            if b == 0.0 {
                Err(div_zero())
            } else {
                finite(a / b)
            }
        }
        BinOp::IntDiv => {
            if b == 0.0 {
                Err(div_zero())
            } else {
                #[allow(clippy::cast_possible_truncation)]
                Ok(V::Int((a / b).floor() as i64))
            }
        }
        BinOp::Mod => {
            if b == 0.0 {
                Err(div_zero())
            } else {
                finite(a % b)
            }
        }
        _ => Ok(V::Null),
    }
}

/// A double as text: whole numbers below 2^53 without a fraction,
/// others in the shortest form that reads back the same.
#[must_use]
pub fn num_str(x: f64) -> String {
    if x == x.floor() && x.abs() < 9_007_199_254_740_992.0 {
        #[allow(clippy::cast_possible_truncation)]
        let i = x as i64;
        i.to_string()
    } else {
        format!("{x}")
    }
}

/// IR `num_str`, and how `Concat` reads its operands.
#[must_use]
pub fn to_str(v: &Value) -> String {
    match v {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(x) => num_str(*x),
        Value::Str(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

/// The longest numeric prefix of `s`, as MariaDB's `CAST(s AS DOUBLE)`
/// reads it (0 when there is none).
fn cast_double(s: &str) -> f64 {
    let t = s.trim_start();
    let mut end = 0;
    let mut best = 0.0;
    for (i, _) in t.char_indices().chain(std::iter::once((t.len(), ' '))) {
        if i == 0 {
            continue;
        }
        if let Ok(x) = t[..i].parse::<f64>()
            && !t[..i].ends_with(['e', 'E', '+', '-'])
            && !t[..i].contains(char::is_alphabetic)
        {
            best = x;
            end = i;
        }
    }
    let _ = end;
    best
}

fn builtin(b: Builtin, a: &[Value]) -> Result<Value> {
    use Value as V;
    let a0 = a.first().cloned().unwrap_or(V::Null);
    let a1 = a.get(1).cloned().unwrap_or(V::Null);
    Ok(match b {
        Builtin::Truthy => V::Bool(truthy(&a0)),
        Builtin::NotNull => {
            if a0.is_null() {
                return Err(runtime(
                    "an arithmetic operator was applied to a null value",
                ));
            }
            a0
        }
        Builtin::Present => V::Bool(match a0 {
            V::Null => false,
            V::Bool(b) => b,
            _ => true,
        }),
        Builtin::ToFloat => match a0 {
            V::Int(i) => V::Float(i as f64),
            v => v,
        },
        Builtin::ToStr => V::Str(to_str(&a0)),
        Builtin::JsonNum => match a0 {
            V::Json(J::Number(n)) => V::Float(n.as_f64().unwrap_or(0.0)),
            V::Json(J::String(s)) => V::Float(cast_double(&s)),
            V::Json(J::Bool(b)) => V::Float(if b { 1.0 } else { 0.0 }),
            V::Int(i) => V::Float(i as f64),
            V::Float(x) => V::Float(x),
            _ => V::Null,
        },
        Builtin::JsonStr => match a0 {
            V::Json(J::String(s)) | V::Str(s) => V::Str(s),
            V::Json(J::Number(n)) => V::Str(n.to_string()),
            V::Json(J::Bool(b)) => V::Str(if b { "1" } else { "0" }.to_owned()),
            _ => V::Null,
        },
        Builtin::ToJson => match a0 {
            V::Null => V::Null,
            v => V::Json(v.to_json()),
        },
        Builtin::JsonLength => match a0 {
            V::Json(J::Array(v)) => V::Int(i64::try_from(v.len()).unwrap_or(i64::MAX)),
            V::Json(J::Object(m)) => V::Int(i64::try_from(m.len()).unwrap_or(i64::MAX)),
            V::Null => V::Null,
            _ => V::Int(1),
        },
        Builtin::Abs => match a0 {
            V::Int(i) => V::Int(i.checked_abs().ok_or_else(overflow)?),
            V::Float(x) => V::Float(x.abs()),
            _ => V::Null,
        },
        Builtin::Floor | Builtin::Ceil | Builtin::Round | Builtin::Sqrt => {
            let Some(x) = num(&a0) else {
                return Ok(V::Null);
            };
            match b {
                Builtin::Floor => V::Float(x.floor()),
                Builtin::Ceil => V::Float(x.ceil()),
                Builtin::Round => V::Float((x + 0.5).floor()),
                _ => {
                    if x < 0.0 {
                        V::Null
                    } else {
                        V::Float(x.sqrt())
                    }
                }
            }
        }
        Builtin::Pow => match (num(&a0), num(&a1)) {
            (Some(x), Some(y)) => finite(x.powf(y))?,
            _ => V::Null,
        },
        Builtin::Max | Builtin::Min => {
            if a.iter().any(Value::is_null) || a.is_empty() {
                return Ok(V::Null);
            }
            if a.iter().all(|v| matches!(v, V::Int(_))) {
                let it = a
                    .iter()
                    .filter_map(|v| if let V::Int(i) = v { Some(*i) } else { None });
                V::Int(
                    if b == Builtin::Max {
                        it.max()
                    } else {
                        it.min()
                    }
                    .unwrap_or(0),
                )
            } else {
                let it = a.iter().filter_map(num);
                let r = if b == Builtin::Max {
                    it.fold(f64::NEG_INFINITY, f64::max)
                } else {
                    it.fold(f64::INFINITY, f64::min)
                };
                V::Float(r)
            }
        }
        Builtin::Len => match a0 {
            V::Str(s) => V::Int(i64::try_from(s.chars().count()).unwrap_or(i64::MAX)),
            _ => V::Null,
        },
        Builtin::Upper => match a0 {
            V::Str(s) => V::Str(s.to_uppercase()),
            _ => V::Null,
        },
        Builtin::Lower => match a0 {
            V::Str(s) => V::Str(s.to_lowercase()),
            _ => V::Null,
        },
        Builtin::Contains | Builtin::StartsWith | Builtin::EndsWith | Builtin::IndexOf => {
            let (V::Str(x), V::Str(y)) = (a0, a1) else {
                return Ok(V::Null);
            };
            match b {
                Builtin::Contains => V::Bool(x.contains(&y)),
                Builtin::StartsWith => V::Bool(x.starts_with(&y)),
                Builtin::EndsWith => V::Bool(x.ends_with(&y)),
                _ => V::Int(match x.find(&y) {
                    Some(byte) => i64::try_from(x[..byte].chars().count()).unwrap_or(i64::MAX),
                    None => -1,
                }),
            }
        }
    })
}

/// A column's text as a value of `ty`.
fn from_text(cell: Option<String>, ty: Ty) -> Result<Value> {
    let Some(s) = cell else {
        return Ok(Value::Null);
    };
    Ok(match ty {
        Ty::Int => Value::Int(
            s.parse()
                .map_err(|_| runtime(format!("not a whole number: {s}")))?,
        ),
        Ty::Float => Value::Float(
            s.parse()
                .map_err(|_| runtime(format!("not a number: {s}")))?,
        ),
        Ty::Bool => Value::Bool(s != "0"),
        Ty::Str => Value::Str(s),
        Ty::Json => Value::from_json(serde_json::from_str(&s).map_err(|e| runtime(e.to_string()))?),
        Ty::Null => Value::Null,
    })
}
