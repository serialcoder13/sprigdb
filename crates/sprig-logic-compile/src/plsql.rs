//! PL/SQL generation: one version's
//! helper package and entry procedure, in MariaDB's `sql_mode=ORACLE`
//! dialect.
//!
//! Rules the generator keeps, each measured on MariaDB 11.8:
//!
//! - a package spec is created before its body, and helpers called from
//!   the procedure are in the spec;
//! - no standalone stored functions (1418, 1419 with the binary log on);
//! - `SIGNAL` takes the message from a variable cut to 128 characters,
//!   and the error number from a parameter;
//! - a routine variable or parameter passed bare to `JSON_OBJECT`,
//!   `JSON_ARRAY` or `JSON_ARRAY_APPEND` is embedded as a JSON string,
//!   whatever its type (measured again for this build: `BIGINT` 7 becomes
//!   `"7"`). So every value is embedded through `embed`: numbers as
//!   `(v + 0)`, JSON as `JSON_EXTRACT(v, '$')`, booleans as a JSON literal;
//! - `JSON_EXTRACT` returns JSON `null` as the text `null`; paths map it
//!   to SQL `NULL` with `JSON_TYPE`, because `NULLIF(…, 'null')` also
//!   matches the JSON string `"null"`;
//! - integer division goes through `* 1e0`, since `/` on integers is
//!   `DECIMAL` with four decimals;
//! - text compares under `utf8mb4_bin`, so by code point;
//! - division by zero (1365) and overflow (1690) are left to MariaDB; the
//!   gateway maps them to 39866.

use std::fmt::Write as _;

use crate::Options;
use crate::ir::{
    BinOp, Bound, Builtin, DbOp, Expr, Function, Literal, LocalId, PathSeg, SqlShape, Stmt, Ty,
    UnOp,
};

/// The `sql_mode` routines are created under (stored with each routine).
pub const SQL_MODE: &str = "ORACLE,STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO";

/// One version's generated text, with logical names: the routines live in
/// each shard schema of the route's group, and their bodies name tables
/// unqualified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plsql {
    /// `__sprig_l<module>_v<version>_lib`.
    pub package: String,
    /// `__sprig_l<module>_v<version>`.
    pub entry: String,
    /// `CREATE OR REPLACE PACKAGE …` (helpers' signatures), when any
    /// helper is used.
    pub package_spec: Option<String>,
    /// `CREATE OR REPLACE PACKAGE BODY …`.
    pub package_body: Option<String>,
    /// `CREATE OR REPLACE PROCEDURE …`: parameters `p_<name>`, then
    /// `p__event OUT VARCHAR(64)` and `p__result OUT LONGTEXT` (the event's
    /// fields as a JSON object).
    pub procedure: String,
    /// Every statement to run, in order, under [`SQL_MODE`].
    pub statements: Vec<String>,
}

/// The routine names of a version.
#[must_use]
pub fn names(module_id: u64, version: u64) -> (String, String) {
    let entry = format!("__sprig_l{module_id}_v{version}");
    (format!("{entry}_lib"), entry)
}

/// Generates PL/SQL for `f`, which must have no placement reasons.
#[must_use]
pub fn generate(f: &Function, opts: &Options) -> Plsql {
    let (package, entry) = names(opts.module_id, opts.version);
    let mut g = Gen {
        f,
        opts,
        pkg: package.clone(),
        uses_fail: false,
        uses_num_str: false,
        uses_nn: false,
        guards: 0,
        params: param_names(f),
    };
    let mut body = String::new();
    g.block(&f.body, 1, &mut body);
    // No event raised: `output` with no fields, unless `output` declares
    // fields (KIRun's "No events raised").
    let output_has_fields = f.event("output").is_some_and(|e| !e.fields.is_empty());
    body.push_str("  IF p__event IS NULL THEN\n");
    if output_has_fields {
        let _ = writeln!(body, "    {};", g.fail(39866, "'No events raised'"));
    } else {
        body.push_str("    p__event := 'output';\n    p__result := JSON_OBJECT();\n");
    }
    body.push_str("  END IF;\n");

    let mut decl = String::new();
    for (i, l) in f.locals.iter().enumerate() {
        let _ = writeln!(
            decl,
            "  {} {};",
            local_name(f, LocalId(u32::try_from(i).unwrap_or(0))),
            sql_type(l.ty)
        );
    }
    for i in 0..g.guards {
        let _ = writeln!(decl, "  v__guard{i} BIGINT := 0;");
    }
    let mut params: Vec<String> = f
        .params
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{} IN {}", g.params[i], sql_type(p.ty)))
        .collect();
    params.push("p__event OUT VARCHAR(64)".to_owned());
    params.push("p__result OUT LONGTEXT".to_owned());
    let security = if opts.definer_security {
        "DEFINER"
    } else {
        "INVOKER"
    };
    let procedure = format!(
        "CREATE OR REPLACE PROCEDURE {entry} (\n    {}\n  )\n  SQL SECURITY {security}\nAS\n{decl}BEGIN\n{body}END",
        params.join(",\n    ")
    );
    let (spec, pbody) = g.package();
    let mut statements = Vec::new();
    if let (Some(s), Some(b)) = (&spec, &pbody) {
        statements.push(s.clone());
        statements.push(b.clone());
    }
    statements.push(procedure.clone());
    Plsql {
        package,
        entry,
        package_spec: spec,
        package_body: pbody,
        procedure,
        statements,
    }
}

/// `p_<name>`, made unique.
fn param_names(f: &Function) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in &f.params {
        let base = format!("p_{}", crate::ir::sanitize(&p.name));
        let mut n = base.clone();
        let mut i = 2;
        while out.contains(&n) {
            n = format!("{base}_{i}");
            i += 1;
        }
        out.push(n);
    }
    out
}

fn local_name(f: &Function, id: LocalId) -> String {
    format!("v_{}", f.local_def(id).name)
}

/// The SQL type of a local or parameter.
#[must_use]
pub fn sql_type(t: Ty) -> &'static str {
    match t {
        Ty::Bool => "BOOLEAN",
        Ty::Int => "BIGINT",
        Ty::Float => "DOUBLE",
        Ty::Str => "LONGTEXT CHARACTER SET utf8mb4",
        Ty::Json | Ty::Null => "LONGTEXT",
    }
}

/// A string literal: quotes doubled, backslashes escaped (`ORACLE` mode
/// keeps backslash escapes).
#[must_use]
pub fn quote(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// A double literal MariaDB reads as `DOUBLE`.
fn float_lit(x: f64) -> String {
    format!("{x:e}")
}

/// A JSON path member, always quoted.
fn path_key(k: &str) -> String {
    format!(".\"{}\"", k.replace('\\', "\\\\").replace('"', "\\\""))
}

struct Gen<'a> {
    f: &'a Function,
    opts: &'a Options,
    pkg: String,
    uses_fail: bool,
    uses_num_str: bool,
    uses_nn: bool,
    guards: usize,
    params: Vec<String>,
}

impl Gen<'_> {
    fn fail(&mut self, code: u16, msg_sql: &str) -> String {
        self.uses_fail = true;
        format!("{}.fail({code}, {msg_sql})", self.pkg)
    }

    fn package(&self) -> (Option<String>, Option<String>) {
        if !self.uses_fail && !self.uses_num_str && !self.uses_nn {
            return (None, None);
        }
        let mut spec = format!("CREATE OR REPLACE PACKAGE {} AS\n", self.pkg);
        let mut body = format!("CREATE OR REPLACE PACKAGE BODY {} AS\n", self.pkg);
        if self.uses_fail {
            spec.push_str("  PROCEDURE fail(code IN INT, msg IN VARCHAR);\n");
            body.push_str(
                "  PROCEDURE fail(code IN INT, msg IN VARCHAR) AS\n    \
                 v VARCHAR(128) := LEFT(msg, 128);\n  \
                 BEGIN\n    SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = code, MESSAGE_TEXT = v;\n  \
                 END;\n",
            );
        }
        if self.uses_num_str {
            spec.push_str("  FUNCTION num_str(x IN DOUBLE) RETURN VARCHAR;\n");
            body.push_str(
                "  FUNCTION num_str(x IN DOUBLE) RETURN VARCHAR AS\n  BEGIN\n    \
                 RETURN CASE WHEN x IS NULL THEN NULL\n                \
                 WHEN x = FLOOR(x) AND ABS(x) < 9007199254740992\n                \
                 THEN CAST(CAST(x AS SIGNED) AS CHAR) ELSE CAST(x AS CHAR) END;\n  \
                 END;\n",
            );
        }
        if self.uses_nn {
            for (f, t) in [("nn_i", "BIGINT"), ("nn_f", "DOUBLE")] {
                spec.push_str(&format!("  FUNCTION {f}(x IN {t}) RETURN {t};\n"));
                body.push_str(&format!(
                    "  FUNCTION {f}(x IN {t}) RETURN {t} AS\n  BEGIN\n    \
                     IF x IS NULL THEN\n      SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = 39866, \
                     MESSAGE_TEXT = 'an arithmetic operator was applied to a null value';\n    \
                     END IF;\n    RETURN x;\n  END;\n"
                ));
            }
        }
        spec.push_str("END");
        body.push_str("END");
        (Some(spec), Some(body))
    }

    fn block(&mut self, b: &[Stmt], depth: usize, out: &mut String) {
        if b.is_empty() {
            let _ = writeln!(out, "{}NULL;", "  ".repeat(depth));
        }
        for s in b {
            self.stmt(s, depth, out);
        }
    }

    #[allow(clippy::too_many_lines)]
    fn stmt(&mut self, s: &Stmt, d: usize, out: &mut String) {
        let ind = "  ".repeat(d);
        match s {
            Stmt::Assign(l, e) => {
                let v = self.expr(e);
                let _ = writeln!(out, "{ind}{} := {v};", local_name(self.f, *l));
            }
            Stmt::If(c, a, b) => {
                let c = self.truthy(c);
                let _ = writeln!(out, "{ind}IF {c} THEN");
                self.block(a, d + 1, out);
                if !b.is_empty() {
                    let _ = writeln!(out, "{ind}ELSE");
                    self.block(b, d + 1, out);
                }
                let _ = writeln!(out, "{ind}END IF;");
            }
            Stmt::While { cond, body, bound } => {
                let c = self.truthy(cond);
                let _ = writeln!(out, "{ind}WHILE {c} LOOP");
                if *bound == Bound::Guarded {
                    let g = format!("v__guard{}", self.guards);
                    self.guards += 1;
                    let fail = self.fail(
                        39864,
                        &quote(&format!(
                            "loop budget: more than {} iterations",
                            self.opts.max_loop_iterations
                        )),
                    );
                    let _ = writeln!(out, "{ind}  {g} := {g} + 1;");
                    let _ = writeln!(
                        out,
                        "{ind}  IF {g} > {} THEN {fail}; END IF;",
                        self.opts.max_loop_iterations
                    );
                }
                self.block(body, d + 1, out);
                let _ = writeln!(out, "{ind}END LOOP;");
            }
            Stmt::Break => {
                let _ = writeln!(out, "{ind}EXIT;");
            }
            // MariaDB's `CONTINUE` (sql_mode=ORACLE): the loop's condition,
            // and its guard, run again.
            Stmt::Continue => {
                let _ = writeln!(out, "{ind}CONTINUE;");
            }
            Stmt::Return => {
                let _ = writeln!(out, "{ind}RETURN;");
            }
            Stmt::Emit(name, fields) => {
                let obj = self.object(fields);
                let _ = writeln!(out, "{ind}IF p__event IS NULL THEN");
                let _ = writeln!(out, "{ind}  p__event := {};", quote(name));
                let _ = writeln!(out, "{ind}  p__result := {obj};");
                let _ = writeln!(out, "{ind}END IF;");
                if name == "output" {
                    let _ = writeln!(out, "{ind}RETURN;");
                }
            }
            Stmt::Throw(e) => {
                let m = self.expr(&e.message);
                let f = self.fail(e.code, &m);
                let _ = writeln!(out, "{ind}{f};");
            }
            Stmt::Db(DbOp::Sql {
                stmt,
                binds,
                shape,
                origin,
            }) => {
                let mut text = String::new();
                for (i, part) in stmt.parts.iter().enumerate() {
                    text.push_str(part);
                    if let Some(b) = binds.get(i) {
                        let v = self.expr(b);
                        let _ = write!(text, "({v})");
                    }
                }
                match shape {
                    SqlShape::One { found, into } => {
                        let cols: Vec<String> = into
                            .iter()
                            .map(|(c, _)| format!("q.`{}`", c.replace('`', "``")))
                            .collect();
                        let vars: Vec<String> =
                            into.iter().map(|(_, l)| local_name(self.f, *l)).collect();
                        let fail = self.fail(
                            39866,
                            &quote(&format!(
                                "Sprig.Db.QueryOne at {origin} returned more than one row"
                            )),
                        );
                        let _ = writeln!(out, "{ind}BEGIN");
                        let _ = writeln!(
                            out,
                            "{ind}  SELECT {} INTO {} FROM ({text}) q;",
                            if cols.is_empty() {
                                "1".to_owned()
                            } else {
                                cols.join(", ")
                            },
                            if vars.is_empty() {
                                "v__unused".to_owned()
                            } else {
                                vars.join(", ")
                            }
                        );
                        let _ = writeln!(out, "{ind}  {} := TRUE;", local_name(self.f, *found));
                        let _ = writeln!(out, "{ind}EXCEPTION");
                        let mut nulls = String::new();
                        for v in &vars {
                            let _ = write!(nulls, " {v} := NULL;");
                        }
                        let _ = writeln!(
                            out,
                            "{ind}  WHEN NO_DATA_FOUND THEN {} := FALSE;{nulls}",
                            local_name(self.f, *found)
                        );
                        let _ = writeln!(out, "{ind}  WHEN TOO_MANY_ROWS THEN {fail};");
                        let _ = writeln!(out, "{ind}END;");
                    }
                    SqlShape::Many { into, cols } => {
                        let v = local_name(self.f, *into);
                        let rec = format!("r_{}", &v[2..]);
                        let members: Vec<String> = cols
                            .iter()
                            .map(|(c, t)| {
                                let field = format!("{rec}.`{}`", c.replace('`', "``"));
                                format!("{}, {}", quote(c), embed_raw(&field, *t))
                            })
                            .collect();
                        // A named cursor in its own block, not `FOR r IN
                        // (SELECT ...)`: MariaDB 11.8 re-parses an inline
                        // cursor's query wrongly after a table it reads is
                        // re-created (1064 near ''), a named one correctly.
                        let cur = format!("c_{}", &v[2..]);
                        let _ = writeln!(out, "{ind}{v} := JSON_ARRAY();");
                        let _ = writeln!(out, "{ind}DECLARE");
                        let _ = writeln!(out, "{ind}  CURSOR {cur} IS {text};");
                        let _ = writeln!(out, "{ind}BEGIN");
                        let _ = writeln!(out, "{ind}  FOR {rec} IN {cur} LOOP");
                        let _ = writeln!(
                            out,
                            "{ind}    {v} := JSON_ARRAY_APPEND({v}, '$', JSON_OBJECT({}));",
                            members.join(", ")
                        );
                        let _ = writeln!(out, "{ind}  END LOOP;");
                        let _ = writeln!(out, "{ind}END;");
                    }
                    SqlShape::Exec { affected } => {
                        let _ = writeln!(out, "{ind}{text};");
                        let _ = writeln!(
                            out,
                            "{ind}{} := ROW_COUNT();",
                            local_name(self.f, *affected)
                        );
                    }
                }
            }
        }
    }

    fn object(&mut self, fields: &[(String, Expr)]) -> String {
        let m: Vec<String> = fields
            .iter()
            .map(|(k, v)| format!("{}, {}", quote(k), self.embed(v)))
            .collect();
        format!("JSON_OBJECT({})", m.join(", "))
    }

    /// `e` as an argument of a JSON function, keeping its type.
    fn embed(&mut self, e: &Expr) -> String {
        let t = self.f.ty_of(e);
        let s = self.expr(e);
        embed_raw(&s, t)
    }

    /// A never-NULL boolean: the operand's truthiness.
    fn truthy(&mut self, e: &Expr) -> String {
        if let Expr::Builtin(Builtin::Truthy, a) = e
            && let [x] = a.as_slice()
        {
            return self.truthy(x);
        }
        let t = self.f.ty_of(e);
        let s = self.expr(e);
        match t {
            Ty::Bool | Ty::Int | Ty::Float => format!("(COALESCE({s}, 0) <> 0)"),
            Ty::Str | Ty::Json => format!("({s} IS NOT NULL)"),
            Ty::Null => "FALSE".to_owned(),
        }
    }

    fn json_path(&mut self, segs: &[PathSeg]) -> String {
        let mut pieces: Vec<String> = Vec::new();
        let mut lit = String::from("$");
        for s in segs {
            match s {
                PathSeg::Key(k) => lit.push_str(&path_key(k)),
                PathSeg::Index(i) => {
                    if let Expr::Lit(Literal::Int(n)) = i.as_ref() {
                        let _ = write!(lit, "[{n}]");
                    } else {
                        lit.push('[');
                        pieces.push(quote(&lit));
                        lit.clear();
                        pieces.push(self.expr(i));
                        lit.push(']');
                    }
                }
            }
        }
        if pieces.is_empty() {
            return quote(&lit);
        }
        pieces.push(quote(&lit));
        format!("CONCAT({})", pieces.join(", "))
    }

    fn str_cmp(&mut self, op: &str, a: &Expr, b: &Expr) -> String {
        let (x, y) = (self.expr(a), self.expr(b));
        if self.f.ty_of(a) == Ty::Str && self.f.ty_of(b) == Ty::Str {
            format!("(({x}) COLLATE utf8mb4_bin {op} ({y}) COLLATE utf8mb4_bin)")
        } else {
            format!("({x} {op} {y})")
        }
    }

    #[allow(clippy::too_many_lines)]
    fn expr(&mut self, e: &Expr) -> String {
        match e {
            Expr::Lit(l) => match l {
                Literal::Null => "NULL".to_owned(),
                Literal::Bool(true) => "TRUE".to_owned(),
                Literal::Bool(false) => "FALSE".to_owned(),
                Literal::Int(i) => {
                    if *i < 0 {
                        format!("({i})")
                    } else {
                        i.to_string()
                    }
                }
                Literal::Float(x) => {
                    if *x < 0.0 {
                        format!("({})", float_lit(*x))
                    } else {
                        float_lit(*x)
                    }
                }
                Literal::Str(s) => quote(s),
                Literal::Json(v) => quote(&v.to_string()),
            },
            Expr::Local(l) => local_name(self.f, *l),
            Expr::Param(i) => self
                .params
                .get(*i as usize)
                .cloned()
                .unwrap_or_else(|| "NULL".to_owned()),
            Expr::Path(b, segs) => {
                let base = self.expr(b);
                let p = self.json_path(segs);
                let x = format!("JSON_EXTRACT({base}, {p})");
                format!("(CASE WHEN JSON_TYPE({x}) = 'NULL' THEN NULL ELSE {x} END)")
            }
            Expr::Unary(UnOp::Neg, a) => format!("(-{})", self.expr(a)),
            Expr::Unary(UnOp::Not, a) => format!("(NOT {})", self.truthy(a)),
            Expr::Binary(op, a, b) => match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul => {
                    let o = match op {
                        BinOp::Add => "+",
                        BinOp::Sub => "-",
                        _ => "*",
                    };
                    format!("({} {o} {})", self.expr(a), self.expr(b))
                }
                BinOp::Div => format!("({} * 1e0 / {})", self.expr(a), self.expr(b)),
                BinOp::IntDiv => format!(
                    "CAST(FLOOR({} * 1e0 / {}) AS SIGNED)",
                    self.expr(a),
                    self.expr(b)
                ),
                BinOp::Mod => format!("MOD({}, {})", self.expr(a), self.expr(b)),
                BinOp::Concat => format!(
                    "CONCAT(COALESCE({}, 'null'), COALESCE({}, 'null'))",
                    self.expr(a),
                    self.expr(b)
                ),
                BinOp::Eq => self.str_cmp("<=>", a, b),
                BinOp::Ne => format!("(NOT {})", self.str_cmp("<=>", a, b)),
                BinOp::Lt => self.str_cmp("<", a, b),
                BinOp::Le => self.str_cmp("<=", a, b),
                BinOp::Gt => self.str_cmp(">", a, b),
                BinOp::Ge => self.str_cmp(">=", a, b),
                BinOp::And => format!("({} AND {})", self.truthy(a), self.truthy(b)),
                BinOp::Or => format!("({} OR {})", self.truthy(a), self.truthy(b)),
                BinOp::Coalesce => format!("COALESCE({}, {})", self.expr(a), self.expr(b)),
            },
            Expr::Cond(c, a, b) => {
                let c = self.truthy(c);
                format!(
                    "(CASE WHEN {c} THEN {} ELSE {} END)",
                    self.expr(a),
                    self.expr(b)
                )
            }
            Expr::Builtin(b, args) => self.builtin(*b, args),
            Expr::ObjectLit(m) => self.object(m),
            Expr::ArrayLit(v) => {
                let a: Vec<String> = v.iter().map(|x| self.embed(x)).collect();
                format!("JSON_ARRAY({})", a.join(", "))
            }
        }
    }

    fn builtin(&mut self, b: Builtin, args: &[Expr]) -> String {
        let a0 = args.first().cloned().unwrap_or(Expr::Lit(Literal::Null));
        let a1 = args.get(1).cloned().unwrap_or(Expr::Lit(Literal::Null));
        match b {
            Builtin::Truthy => self.truthy(&a0),
            Builtin::Present => {
                let t = self.f.ty_of(&a0);
                let s = self.expr(&a0);
                match t {
                    Ty::Bool => format!("(COALESCE({s}, 0) <> 0)"),
                    Ty::Null => "FALSE".to_owned(),
                    _ => format!("({s} IS NOT NULL)"),
                }
            }
            Builtin::ToFloat => format!("({} + 0e0)", self.expr(&a0)),
            Builtin::ToStr => {
                let t = self.f.ty_of(&a0);
                let s = self.expr(&a0);
                match t {
                    Ty::Int => format!("COALESCE(CAST({s} AS CHAR), 'null')"),
                    Ty::Float => {
                        self.uses_num_str = true;
                        format!("COALESCE({}.num_str({s}), 'null')", self.pkg)
                    }
                    Ty::Bool => format!(
                        "(CASE WHEN {s} IS NULL THEN 'null' WHEN {s} THEN 'true' ELSE 'false' END)"
                    ),
                    Ty::Null => "'null'".to_owned(),
                    Ty::Str | Ty::Json => s,
                }
            }
            Builtin::JsonNum => format!("CAST(JSON_VALUE({}, '$') AS DOUBLE)", self.expr(&a0)),
            Builtin::JsonStr => format!("JSON_VALUE({}, '$')", self.expr(&a0)),
            Builtin::ToJson => {
                let t = self.f.ty_of(&a0);
                let s = self.expr(&a0);
                if t == Ty::Json {
                    return s;
                }
                let inner = embed_raw(&s, t);
                format!(
                    "(CASE WHEN {s} IS NULL THEN NULL ELSE JSON_EXTRACT(JSON_ARRAY({inner}), '$[0]') END)"
                )
            }
            Builtin::JsonLength => format!("JSON_LENGTH({})", self.expr(&a0)),
            Builtin::Abs => format!("ABS({})", self.expr(&a0)),
            Builtin::Floor => format!("(FLOOR({}) + 0e0)", self.expr(&a0)),
            Builtin::Ceil => format!("(CEIL({}) + 0e0)", self.expr(&a0)),
            Builtin::Round => format!("(FLOOR({} + 5e-1) + 0e0)", self.expr(&a0)),
            Builtin::Sqrt => format!("SQRT({})", self.expr(&a0)),
            Builtin::NotNull => {
                self.uses_nn = true;
                let f = if self.f.ty_of(&a0) == Ty::Int {
                    "nn_i"
                } else {
                    "nn_f"
                };
                let x = self.expr(&a0);
                format!("{}.{f}({x})", self.pkg)
            }
            Builtin::Pow => format!("POW({}, {})", self.expr(&a0), self.expr(&a1)),
            Builtin::Max | Builtin::Min => {
                let f = if b == Builtin::Max {
                    "GREATEST"
                } else {
                    "LEAST"
                };
                let a: Vec<String> = args.iter().map(|x| self.expr(x)).collect();
                if a.len() == 1 {
                    return a[0].clone();
                }
                format!("{f}({})", a.join(", "))
            }
            Builtin::Len => format!("CHAR_LENGTH({})", self.expr(&a0)),
            Builtin::Upper => format!("UPPER({})", self.expr(&a0)),
            Builtin::Lower => format!("LOWER({})", self.expr(&a0)),
            Builtin::Contains => format!(
                "(LOCATE(({}) COLLATE utf8mb4_bin, ({}) COLLATE utf8mb4_bin) > 0)",
                self.expr(&a1),
                self.expr(&a0)
            ),
            Builtin::StartsWith => {
                let (x, y) = (self.expr(&a0), self.expr(&a1));
                format!(
                    "((LEFT({x}, CHAR_LENGTH({y}))) COLLATE utf8mb4_bin = ({y}) COLLATE utf8mb4_bin)"
                )
            }
            Builtin::EndsWith => {
                let (x, y) = (self.expr(&a0), self.expr(&a1));
                format!(
                    "((RIGHT({x}, CHAR_LENGTH({y}))) COLLATE utf8mb4_bin = ({y}) COLLATE utf8mb4_bin)"
                )
            }
            Builtin::IndexOf => format!(
                "(LOCATE(({}) COLLATE utf8mb4_bin, ({}) COLLATE utf8mb4_bin) - 1)",
                self.expr(&a1),
                self.expr(&a0)
            ),
        }
    }
}

/// `s` (SQL of type `t`) as a JSON function argument (see the module
/// docs).
#[must_use]
pub fn embed_raw(s: &str, t: Ty) -> String {
    match t {
        Ty::Int => format!("({s} + 0)"),
        Ty::Float => format!("({s} + 0e0)"),
        Ty::Str => s.to_owned(),
        Ty::Bool => format!(
            "JSON_EXTRACT(CASE WHEN {s} IS NULL THEN 'null' WHEN {s} THEN 'true' ELSE 'false' END, '$')"
        ),
        Ty::Json => format!("JSON_EXTRACT({s}, '$')"),
        Ty::Null => "NULL".to_owned(),
    }
}
