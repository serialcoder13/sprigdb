//! Functions as plain SQL expressions, for validations and computed
//! fields.
//!
//! MariaDB forbids stored routines in `CHECK` constraints and generated
//! columns, so a validation becomes a `CHECK` and a computed field a
//! generated column only when the whole body can be written as one
//! expression over the row's columns. This module tries that by symbolic
//! execution of the IR:
//!
//! ```text
//! paths(f):
//!   run the body once per path: an `If` splits the path, its condition
//!   (truthiness, NULL as false) joining the path's conditions; locals are
//!   SQL expressions over the parameters; the first event raised is the
//!   path's result; `output`, `Throw` and `Return` end the path
//!   loops, data access, JSON values, `/` by anything but a nonzero
//!   constant, and more than 64 paths: not expressible (an error naming why)
//! computed(f, args)  = CASE WHEN path₁ THEN value₁ ... END   (output's one field)
//! reject_check(f, …) = OR of the conditions of the paths raising `reject`
//!                      (one static message; JavaScript's `sprig.reject`,
//!                      error 39863, counts as one), when no path throws
//! ```
//!
//! Every built-in used here is deterministic, as both places require.
//! Text compares by code point, as in compiled code, through
//! `CONVERT(… USING utf8mb4) COLLATE utf8mb4_bin`.
//!
//! **Known difference.** KIRun's arithmetic fails on a `NULL` operand
//! (the IR's `NotNull`); an expression can't fail, so a computed field
//! gives `NULL` there. Validations don't take this form when the body has
//! such a check: they become triggers, which keep KIRun's behaviour.

use crate::ir::{BinOp, Builtin, Expr, Function, Literal, Stmt, Ty, UnOp};

/// The most paths one body may have.
pub const MAX_PATHS: usize = 64;

/// A SQL expression and its IR type.
#[derive(Debug, Clone, PartialEq)]
pub struct Sql {
    /// The text, default `sql_mode`.
    pub text: String,
    /// Its type.
    pub ty: Ty,
}

impl Sql {
    fn new(text: impl Into<String>, ty: Ty) -> Self {
        Self {
            text: text.into(),
            ty,
        }
    }

    fn null() -> Self {
        Self::new("NULL", Ty::Null)
    }
}

/// How one path of the body ends.
#[derive(Debug, Clone, PartialEq)]
pub enum End {
    /// No event and no error.
    Nothing,
    /// The first event raised, with its fields.
    Event(String, Vec<(String, Sql)>),
    /// An error the code raises: the number and the message expression.
    Throw(u16, Sql),
}

/// One path through the body.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// Conditions, each never `NULL`, that select this path.
    pub conds: Vec<String>,
    /// How it ends.
    pub end: End,
}

impl Path {
    /// The path's condition as one expression (`TRUE` when it has none).
    #[must_use]
    pub fn condition(&self) -> String {
        if self.conds.is_empty() {
            "TRUE".to_owned()
        } else {
            format!("({})", self.conds.join(" AND "))
        }
    }
}

/// The paths of a body, and whether a `NotNull` check was dropped.
#[derive(Debug, Clone, PartialEq)]
pub struct Paths {
    /// Every path, in source order.
    pub paths: Vec<Path>,
    /// Whether some expression had a `NotNull` (KIRun's null check on an
    /// arithmetic operand), which the expression form drops.
    pub dropped_null_checks: bool,
}

#[derive(Clone)]
struct St {
    env: Vec<Option<Sql>>,
    conds: Vec<String>,
    end: Option<End>,
    done: bool,
}

struct Cx<'a> {
    f: &'a Function,
    params: &'a [Option<Sql>],
    not_null: bool,
}

type R<T> = std::result::Result<T, String>;

/// A string literal (default `sql_mode`, so backslashes are escaped).
#[must_use]
pub fn str_lit(s: &str) -> String {
    format!("_utf8mb4'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn bin_str(x: &str) -> String {
    format!("CONVERT({x} USING utf8mb4) COLLATE utf8mb4_bin")
}

/// Truthiness, never `NULL`: `NULL` and `false` are false, numbers unless
/// zero, text and JSON whenever present.
fn truthy(x: &Sql) -> String {
    match x.ty {
        Ty::Bool | Ty::Int | Ty::Float => format!("(({}) IS TRUE)", x.text),
        Ty::Str | Ty::Json => format!("(({}) IS NOT NULL)", x.text),
        Ty::Null => "FALSE".to_owned(),
    }
}

fn literal(l: &Literal) -> R<Sql> {
    Ok(match l {
        Literal::Null => Sql::null(),
        Literal::Bool(b) => Sql::new(if *b { "TRUE" } else { "FALSE" }, Ty::Bool),
        Literal::Int(i) => Sql::new(format!("({i})"), Ty::Int),
        Literal::Float(x) => {
            if !x.is_finite() {
                return Err("a non-finite constant".into());
            }
            Sql::new(format!("({x:e})"), Ty::Float)
        }
        Literal::Str(s) => Sql::new(str_lit(s), Ty::Str),
        Literal::Json(_) => return Err("a JSON value".into()),
    })
}

fn nonzero_const(e: &Expr) -> bool {
    match e {
        Expr::Lit(Literal::Int(i)) => *i != 0,
        Expr::Lit(Literal::Float(x)) => *x != 0.0 && x.is_finite(),
        _ => false,
    }
}

impl Cx<'_> {
    fn expr(&mut self, e: &Expr, st: &St) -> R<Sql> {
        Ok(match e {
            Expr::Lit(l) => literal(l)?,
            Expr::Local(id) => {
                let i = id.0 as usize;
                match st.env.get(i).cloned().flatten() {
                    Some(v) => v,
                    None => Sql::new("NULL", self.f.local_def(*id).ty),
                }
            }
            Expr::Param(i) => self
                .params
                .get(usize::from(*i))
                .cloned()
                .flatten()
                .ok_or_else(|| {
                    let n = self
                        .f
                        .params
                        .get(usize::from(*i))
                        .map_or("?", |p| p.name.as_str());
                    format!("parameter {n} has no value here")
                })?,
            Expr::Path(..) | Expr::ObjectLit(_) | Expr::ArrayLit(_) => {
                return Err("JSON values".into());
            }
            Expr::Unary(UnOp::Neg, a) => {
                let a = self.expr(a, st)?;
                Sql::new(format!("(-({}))", a.text), a.ty)
            }
            Expr::Unary(UnOp::Not, a) => {
                let a = self.expr(a, st)?;
                Sql::new(format!("(NOT {})", truthy(&a)), Ty::Bool)
            }
            Expr::Binary(op, a, b) => self.binary(*op, a, b, st)?,
            Expr::Cond(c, a, b) => {
                let c = self.expr(c, st)?;
                let a = self.expr(a, st)?;
                let b = self.expr(b, st)?;
                let ty = a.ty.join(b.ty).ok_or("branches of different types")?;
                Sql::new(
                    format!(
                        "(CASE WHEN {} THEN {} ELSE {} END)",
                        truthy(&c),
                        a.text,
                        b.text
                    ),
                    ty,
                )
            }
            Expr::Builtin(b, args) => self.builtin(*b, args, st)?,
        })
    }

    fn binary(&mut self, op: BinOp, a: &Expr, b: &Expr, st: &St) -> R<Sql> {
        let divisor_ok = nonzero_const(b);
        let x = self.expr(a, st)?;
        let y = self.expr(b, st)?;
        let num = |t: Ty| t.is_number() || t == Ty::Null;
        Ok(match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul => {
                if !num(x.ty) || !num(y.ty) {
                    return Err(format!("{} of non-numbers", op.name()));
                }
                let sym = match op {
                    BinOp::Add => "+",
                    BinOp::Sub => "-",
                    _ => "*",
                };
                let ty = if x.ty == Ty::Int && y.ty == Ty::Int {
                    Ty::Int
                } else {
                    Ty::Float
                };
                Sql::new(format!("({} {sym} {})", x.text, y.text), ty)
            }
            BinOp::Div => {
                if !divisor_ok {
                    return Err("division by a value that may be zero".into());
                }
                Sql::new(format!("(({}) * 1e0 / {})", x.text, y.text), Ty::Float)
            }
            BinOp::IntDiv => {
                if !divisor_ok {
                    return Err("division by a value that may be zero".into());
                }
                Sql::new(format!("FLOOR(({}) * 1e0 / {})", x.text, y.text), Ty::Int)
            }
            BinOp::Mod => {
                if !divisor_ok {
                    return Err("remainder by a value that may be zero".into());
                }
                let ty = x.ty.join(y.ty).unwrap_or(Ty::Float);
                Sql::new(format!("MOD({}, {})", x.text, y.text), ty)
            }
            BinOp::Concat => Sql::new(
                format!(
                    "CONCAT(COALESCE({}, 'null'), COALESCE({}, 'null'))",
                    x.text, y.text
                ),
                Ty::Str,
            ),
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                let text = matches!(x.ty, Ty::Str) || matches!(y.ty, Ty::Str);
                if text && (x.ty.is_number() || y.ty.is_number()) {
                    return Err("comparing text with a number".into());
                }
                if matches!(x.ty, Ty::Json) || matches!(y.ty, Ty::Json) {
                    return Err("JSON values".into());
                }
                let (l, r) = if text {
                    (bin_str(&x.text), bin_str(&y.text))
                } else {
                    (x.text, y.text)
                };
                let s = match op {
                    BinOp::Eq => format!("({l} <=> {r})"),
                    BinOp::Ne => format!("(NOT ({l} <=> {r}))"),
                    BinOp::Lt => format!("({l} < {r})"),
                    BinOp::Le => format!("({l} <= {r})"),
                    BinOp::Gt => format!("({l} > {r})"),
                    _ => format!("({l} >= {r})"),
                };
                Sql::new(s, Ty::Bool)
            }
            BinOp::And => Sql::new(format!("({} AND {})", truthy(&x), truthy(&y)), Ty::Bool),
            BinOp::Or => Sql::new(format!("({} OR {})", truthy(&x), truthy(&y)), Ty::Bool),
            BinOp::Coalesce => {
                let ty = x.ty.join(y.ty).ok_or("?? of different types")?;
                Sql::new(format!("COALESCE({}, {})", x.text, y.text), ty)
            }
        })
    }

    fn builtin(&mut self, b: Builtin, args: &[Expr], st: &St) -> R<Sql> {
        let mut v = Vec::with_capacity(args.len());
        for a in args {
            v.push(self.expr(a, st)?);
        }
        let one = |v: &[Sql]| -> R<Sql> { v.first().cloned().ok_or_else(|| "an argument".into()) };
        let two = |v: &[Sql]| -> R<(Sql, Sql)> {
            match v {
                [a, b] => Ok((a.clone(), b.clone())),
                _ => Err("two arguments".into()),
            }
        };
        Ok(match b {
            Builtin::Truthy => Sql::new(truthy(&one(&v)?), Ty::Bool),
            Builtin::Present => {
                let x = one(&v)?;
                let s = if x.ty == Ty::Bool {
                    format!("(({}) IS TRUE)", x.text)
                } else {
                    format!("(({}) IS NOT NULL)", x.text)
                };
                Sql::new(s, Ty::Bool)
            }
            Builtin::NotNull => {
                self.not_null = true;
                one(&v)?
            }
            Builtin::ToFloat => Sql::new(format!("(({}) + 0e0)", one(&v)?.text), Ty::Float),
            Builtin::ToStr => {
                let x = one(&v)?;
                let s = match x.ty {
                    Ty::Int => format!("COALESCE(CAST({} AS CHAR), 'null')", x.text),
                    Ty::Bool => format!(
                        "(CASE WHEN ({0}) IS NULL THEN 'null' WHEN ({0}) THEN 'true' ELSE 'false' END)",
                        x.text
                    ),
                    Ty::Str => format!("COALESCE({}, 'null')", x.text),
                    Ty::Null => "'null'".to_owned(),
                    Ty::Float | Ty::Json => return Err("number-to-text of a double".into()),
                };
                Sql::new(s, Ty::Str)
            }
            Builtin::Abs => {
                let x = one(&v)?;
                Sql::new(format!("ABS({})", x.text), x.ty)
            }
            Builtin::Floor => Sql::new(format!("FLOOR({})", one(&v)?.text), Ty::Float),
            Builtin::Ceil => Sql::new(format!("CEILING({})", one(&v)?.text), Ty::Float),
            Builtin::Round => Sql::new(format!("FLOOR(({}) + 0.5e0)", one(&v)?.text), Ty::Float),
            Builtin::Sqrt => Sql::new(format!("SQRT({})", one(&v)?.text), Ty::Float),
            Builtin::Pow => {
                let (a, b) = two(&v)?;
                Sql::new(format!("POW({}, {})", a.text, b.text), Ty::Float)
            }
            Builtin::Max | Builtin::Min => {
                if v.len() < 2 {
                    return one(&v);
                }
                let ty = v
                    .iter()
                    .try_fold(Ty::Null, |t, x| t.join(x.ty))
                    .ok_or("arguments of different types")?;
                let f = if b == Builtin::Max {
                    "GREATEST"
                } else {
                    "LEAST"
                };
                let list: Vec<&str> = v.iter().map(|x| x.text.as_str()).collect();
                Sql::new(format!("{f}({})", list.join(", ")), ty)
            }
            Builtin::Len => Sql::new(format!("CHAR_LENGTH({})", one(&v)?.text), Ty::Int),
            Builtin::Upper => Sql::new(format!("UPPER({})", one(&v)?.text), Ty::Str),
            Builtin::Lower => Sql::new(format!("LOWER({})", one(&v)?.text), Ty::Str),
            Builtin::Contains => {
                let (a, b) = two(&v)?;
                Sql::new(
                    format!("(LOCATE({}, {}) > 0)", bin_str(&b.text), bin_str(&a.text)),
                    Ty::Bool,
                )
            }
            Builtin::StartsWith | Builtin::EndsWith => {
                let (a, y) = two(&v)?;
                let f = if b == Builtin::StartsWith {
                    "LEFT"
                } else {
                    "RIGHT"
                };
                Sql::new(
                    format!(
                        "({f}({}, CHAR_LENGTH({})) = {})",
                        bin_str(&a.text),
                        y.text,
                        bin_str(&y.text)
                    ),
                    Ty::Bool,
                )
            }
            Builtin::IndexOf => {
                let (a, b) = two(&v)?;
                Sql::new(
                    format!("(LOCATE({}, {}) - 1)", bin_str(&b.text), bin_str(&a.text)),
                    Ty::Int,
                )
            }
            Builtin::JsonNum | Builtin::JsonStr | Builtin::ToJson | Builtin::JsonLength => {
                return Err("JSON values".into());
            }
        })
    }

    fn block(&mut self, block: &[Stmt], start: St) -> R<Vec<St>> {
        let mut states = vec![start];
        for stmt in block {
            let mut next = Vec::new();
            for mut s in states {
                if s.done {
                    next.push(s);
                    continue;
                }
                match stmt {
                    Stmt::Assign(id, e) => {
                        let v = self.expr(e, &s)?;
                        let i = id.0 as usize;
                        if s.env.len() <= i {
                            s.env.resize(i + 1, None);
                        }
                        s.env[i] = Some(v);
                        next.push(s);
                    }
                    Stmt::If(c, a, b) => {
                        let c = self.expr(c, &s)?;
                        let t = truthy(&c);
                        let mut sa = s.clone();
                        sa.conds.push(t.clone());
                        let mut sb = s;
                        sb.conds.push(format!("(NOT {t})"));
                        next.extend(self.block(a, sa)?);
                        next.extend(self.block(b, sb)?);
                    }
                    Stmt::While { .. } | Stmt::Break | Stmt::Continue => {
                        return Err("a loop".into());
                    }
                    Stmt::Db(_) => return Err("data access".into()),
                    Stmt::Emit(name, fields) => {
                        if s.end.is_none() {
                            let mut fs = Vec::with_capacity(fields.len());
                            for (n, e) in fields {
                                fs.push((n.clone(), self.expr(e, &s)?));
                            }
                            s.end = Some(End::Event(name.clone(), fs));
                        }
                        if name == "output" {
                            s.done = true;
                        }
                        next.push(s);
                    }
                    Stmt::Throw(spec) => {
                        let m = self.expr(&spec.message, &s)?;
                        s.end = Some(End::Throw(spec.code, m));
                        s.done = true;
                        next.push(s);
                    }
                    Stmt::Return => {
                        s.done = true;
                        next.push(s);
                    }
                }
                if next.len() > MAX_PATHS {
                    return Err(format!("more than {MAX_PATHS} paths"));
                }
            }
            states = next;
        }
        Ok(states)
    }
}

/// The paths of `f`, with parameter `i` bound to `params[i]` (`None`: the
/// parameter has no value in this form, and using it is an error).
///
/// # Errors
/// Why the body has no expression form.
pub fn paths(f: &Function, params: &[Option<Sql>]) -> std::result::Result<Paths, String> {
    let mut cx = Cx {
        f,
        params,
        not_null: false,
    };
    let start = St {
        env: vec![None; f.locals.len()],
        conds: Vec::new(),
        end: None,
        done: false,
    };
    let states = cx.block(&f.body, start)?;
    let output_fields = f.event("output").is_some_and(|e| !e.fields.is_empty());
    let paths = states
        .into_iter()
        .map(|s| Path {
            conds: s.conds,
            end: match s.end {
                Some(e) => e,
                // KIRun's "No events raised" when `output` declares fields.
                None if output_fields => {
                    End::Throw(39866, Sql::new(str_lit("No events raised"), Ty::Str))
                }
                None => End::Nothing,
            },
        })
        .collect();
    Ok(Paths {
        paths,
        dropped_null_checks: cx.not_null,
    })
}

/// A computed field's expression: the value of `output`'s one field on
/// each path. A path that raises 39866 (an argument check, or no event)
/// gives `NULL`, since an expression can't fail.
///
/// # Errors
/// Why there is no expression form.
pub fn computed(f: &Function, args: &[Option<Sql>]) -> std::result::Result<Sql, String> {
    let out = f
        .event("output")
        .ok_or("the function has no output event")?;
    let [(field, _)] = out.fields.as_slice() else {
        return Err("output must have exactly one field, the value".into());
    };
    let p = paths(f, args)?;
    let mut arms = Vec::new();
    let mut ty = Ty::Null;
    for path in &p.paths {
        let v = match &path.end {
            End::Event(name, fields) if name == "output" => fields
                .iter()
                .find(|(n, _)| n == field)
                .map_or_else(Sql::null, |(_, v)| v.clone()),
            End::Event(name, _) => {
                return Err(format!("a path raises the event {name}"));
            }
            End::Throw(39866, _) | End::Nothing => Sql::null(),
            End::Throw(code, _) => return Err(format!("a path raises error {code}")),
        };
        ty = ty
            .join(v.ty)
            .ok_or("paths give values of different types")?;
        arms.push((path.condition(), v.text));
    }
    let text = match arms.as_slice() {
        [] => "NULL".to_owned(),
        [(_, v)] => v.clone(),
        _ => {
            let mut s = "(CASE".to_owned();
            let last = arms.len() - 1;
            for (i, (c, v)) in arms.iter().enumerate() {
                if i == last {
                    s.push_str(&format!(" ELSE {v}"));
                } else {
                    s.push_str(&format!(" WHEN {c} THEN {v}"));
                }
            }
            s.push_str(" END)");
            s
        }
    };
    Ok(Sql::new(text, ty))
}

/// A validation as a `CHECK`: the condition under which it rejects, and
/// its one static message. Only when every failing path raises `reject`
/// with the same literal `message`, no path throws, and no null check of
/// KIRun's arithmetic was dropped; otherwise the validation needs
/// triggers, which keep every behaviour.
///
/// # Errors
/// Why there is no `CHECK` form.
pub fn reject_check(
    f: &Function,
    row: &[Option<Sql>],
) -> std::result::Result<(String, String), String> {
    let p = paths(f, row)?;
    if p.dropped_null_checks {
        return Err("arithmetic that fails on NULL".into());
    }
    let mut conds = Vec::new();
    let mut message: Option<String> = None;
    for path in &p.paths {
        let rejected = match &path.end {
            End::Event(name, fields) if name == "reject" => Some(
                fields
                    .iter()
                    .find(|(n, _)| n == "message")
                    .map_or_else(|| str_lit("rejected"), |(_, v)| v.text.clone()),
            ),
            // JavaScript's `sprig.reject(message)`.
            End::Throw(39863, m) => Some(m.text.clone()),
            _ => None,
        };
        if let Some(m) = rejected {
            let lit = static_text(&m).ok_or("a message that isn't constant")?;
            match &message {
                Some(x) if *x != lit => return Err("more than one message".into()),
                _ => message = Some(lit),
            }
            conds.push(path.condition());
            continue;
        }
        match &path.end {
            End::Throw(code, _) => return Err(format!("a path raises error {code}")),
            End::Event(..) | End::Nothing => {}
        }
    }
    let cond = if conds.is_empty() {
        "FALSE".to_owned()
    } else {
        conds.join(" OR ")
    };
    Ok((cond, message.unwrap_or_else(|| "rejected".to_owned())))
}

/// The text of a string literal made by [`str_lit`].
fn static_text(sql: &str) -> Option<String> {
    let inner = sql.strip_prefix("_utf8mb4'")?.strip_suffix('\'')?;
    if inner.contains('\'') && !inner.contains("''") {
        return None;
    }
    Some(inner.replace("''", "'").replace("\\\\", "\\"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{Options, Runtime, StaticColumns, compile};

    fn ir(def: &str) -> Function {
        compile(
            Runtime::Kirun,
            def,
            &StaticColumns::new(),
            &Options::default(),
        )
        .unwrap()
        .ir
        .unwrap()
    }

    #[allow(clippy::unnecessary_wraps)] // parameters bind as Option<Sql>
    fn col(name: &str, ty: Ty) -> Option<Sql> {
        Some(Sql::new(format!("`{name}`"), ty))
    }

    const DISCOUNT: &str = r#"{"namespace": "Shop", "name": "Discount",
      "parameters": {
        "total": {"parameterName": "total", "schema": {"type": ["DOUBLE", "NULL"]}},
        "discount_pct": {"parameterName": "discount_pct", "schema": {"type": ["DOUBLE", "NULL"]}}},
      "events": {"output": {"name": "output", "parameters": {}},
                 "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
      "steps": {
        "big": {"statementName": "big", "namespace": "System", "name": "If",
          "parameterMap": {"condition": {"c": {"type": "EXPRESSION",
            "expression": "Arguments.discount_pct > 20"}}}},
        "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
          "executeIftrue": {"Steps.big.true": true},
          "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
            "results": {"r": {"type": "VALUE", "value": {"name": "message",
              "value": {"isExpression": false, "value": "discount above 20%"}}}}}}}}"#;

    #[test]
    fn a_static_rule_is_a_check() {
        let f = ir(DISCOUNT);
        let (cond, msg) = reject_check(
            &f,
            &[col("total", Ty::Float), col("discount_pct", Ty::Float)],
        )
        .unwrap();
        assert_eq!(msg, "discount above 20%");
        assert!(cond.contains("`discount_pct` > (20)"), "{cond}");
    }

    #[test]
    fn non_null_parameters_need_triggers() {
        let f = ir(&DISCOUNT.replace(r#"["DOUBLE", "NULL"]"#, r#"["DOUBLE"]"#));
        let e = reject_check(
            &f,
            &[col("total", Ty::Float), col("discount_pct", Ty::Float)],
        )
        .unwrap_err();
        assert!(e.contains("39866"), "{e}");
    }

    const NET: &str = r#"{"namespace": "Shop", "name": "Net",
      "parameters": {
        "price": {"parameterName": "price", "schema": {"type": ["DOUBLE"]}},
        "qty": {"parameterName": "qty", "schema": {"type": ["LONG"]}}},
      "events": {"output": {"name": "output", "parameters": {"value": {"type": ["DOUBLE"]}}}},
      "steps": {
        "bulk": {"statementName": "bulk", "namespace": "System", "name": "If",
          "parameterMap": {"condition": {"c": {"type": "EXPRESSION",
            "expression": "Arguments.qty >= 10"}}}},
        "cheap": {"statementName": "cheap", "namespace": "System", "name": "GenerateEvent",
          "executeIftrue": {"Steps.bulk.true": true},
          "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "value",
              "value": {"isExpression": true, "value": "Arguments.price * Arguments.qty * 0.9"}}}}}},
        "full": {"statementName": "full", "namespace": "System", "name": "GenerateEvent",
          "executeIftrue": {"Steps.bulk.false": true},
          "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "value",
              "value": {"isExpression": true, "value": "Arguments.price * Arguments.qty"}}}}}}}}"#;

    #[test]
    fn a_computed_field_is_a_case() {
        let f = ir(NET);
        let s = computed(&f, &[col("price", Ty::Float), col("qty", Ty::Int)]).unwrap();
        assert_eq!(s.ty, Ty::Float);
        assert!(s.text.starts_with("(CASE WHEN"), "{}", s.text);
        assert!(s.text.contains("`price`"), "{}", s.text);
        assert!(s.text.contains("(9e-1)"), "{}", s.text);
    }

    #[test]
    fn data_access_has_no_expression() {
        let def = r#"{"namespace": "S", "name": "Q",
          "parameters": {"id": {"parameterName": "id", "schema": {"type": ["LONG"]}}},
          "events": {"output": {"name": "output", "parameters": {"value": {"type": ["LONG"]}}}},
          "steps": {"q": {"statementName": "q", "namespace": "Sprig.Db", "name": "QueryOne",
            "parameterMap": {"sql": {"s": {"type": "VALUE", "value": "SELECT 1 AS one"}}}}}}"#;
        let f = compile(
            Runtime::Kirun,
            def,
            &StaticColumns::new(),
            &Options::default(),
        )
        .unwrap()
        .ir
        .unwrap();
        assert!(computed(&f, &[col("id", Ty::Int)]).is_err());
    }

    #[test]
    fn literals_and_text() {
        assert_eq!(str_lit("it's \\ ok"), "_utf8mb4'it''s \\\\ ok'");
        assert_eq!(static_text(&str_lit("it's ok")).as_deref(), Some("it's ok"));
    }
}
