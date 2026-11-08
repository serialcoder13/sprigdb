//! `lower_kirun` (chapter 41, "KIRun front end"): the step graph to
//! structured IR.
//!
//! ```text
//! lower(def):
//!   1 deps(s) := dependentStatements, executeIftrue keys and every
//!     Steps.<t>.<event> s's expressions read; unknown steps and cycles are
//!     syntax errors (39861)
//!   2 B(c, e) := every step reachable from c's event e (e ≠ output), the
//!     sub-graph KIRun re-runs per event; the B sets must nest
//!   3 a step's scope is the smallest B holding it; each scope's steps run
//!     in topological order, ties by statement name
//!   4 a step runs when its dependencies ran and its executeIftrue
//!     expressions are present: a guard and a `ran_<step>` flag when that
//!     isn't known statically
//!   5 each function lowers to statements; If → if, loops → while with a
//!     counter and a break flag, GenerateEvent → emit, Context → locals,
//!     Sprig.Db.* → db statements into scalarized locals
//! ```
//!
//! What has no compiled form becomes a placement [`Reason`], so the
//! function still deploys, in the engine tier.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use sprig_common::{Result, SprigCode, SprigError};

use super::expr::{self, KBin, KExpr, KUn, Num};
use super::model::{Definition, ParamRef, RefKind, SchemaDef, Step};
use crate::Reason;
use crate::ir::{
    BinOp, Bound, Builtin, DbOp, EventDef, Expr, Function, Literal, LocalId, Param, PathSeg,
    SqlShape, Stmt, Ty, UnOp,
};
use crate::sql::{self, Columns, Want};

/// The result of lowering.
#[derive(Debug, Clone)]
pub struct Lowered {
    /// The function. When `reasons` is not empty, parts of it are
    /// placeholders and it only serves `EXPLAIN`.
    pub func: Function,
    /// Why it can't compile; empty when it can.
    pub reasons: Vec<Reason>,
    /// Warnings that don't change the tier.
    pub lints: Vec<Reason>,
}

/// (step, event).
type Ev = (String, String);

const IF: &str = "System.If";
const RANGE: &str = "System.Loop.RangeLoop";
const COUNT: &str = "System.Loop.CountLoop";
const FOREACH: &str = "System.Loop.ForEachLoop";

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSyntax, msg)
}

/// A value an event exposes to expressions.
#[derive(Debug, Clone)]
enum Sym {
    E(Expr),
    /// A scalarized row: `found` and one local per column.
    Row {
        found: LocalId,
        cols: Vec<(String, LocalId)>,
    },
}

/// Lowers a definition. `cat` types the columns of embedded SQL.
///
/// # Errors
/// 39861 for definitions KIRun itself would reject: unknown steps,
/// cycles, unparsable expressions, malformed parameters.
pub fn lower(def: &Definition, cat: &dyn Columns) -> Result<Lowered> {
    let mut l = L {
        def,
        cat,
        f: Function {
            name: if def.namespace.is_empty() {
                def.name.clone()
            } else {
                format!("{}.{}", def.namespace, def.name)
            },
            ..Function::default()
        },
        reasons: Vec::new(),
        lints: Vec::new(),
        deps: BTreeMap::new(),
        branch: BTreeMap::new(),
        owner: BTreeMap::new(),
        syms: BTreeMap::new(),
        ran: BTreeMap::new(),
        ctx: BTreeMap::new(),
        scope: Vec::new(),
        loops: BTreeMap::new(),
        writes: BTreeSet::new(),
    };
    if !def.parts.is_empty() {
        l.reason(
            "kirun.parts",
            "definition",
            "sub-function parts have no compiled form",
        );
    }
    l.signature();
    l.graph()?;
    let body = l.scope_block(None)?;
    l.f.body = body;
    Ok(Lowered {
        func: l.f,
        reasons: l.reasons,
        lints: l.lints,
    })
}

struct L<'a> {
    def: &'a Definition,
    cat: &'a dyn Columns,
    f: Function,
    reasons: Vec<Reason>,
    lints: Vec<Reason>,
    deps: BTreeMap<String, BTreeSet<Ev>>,
    branch: BTreeMap<Ev, BTreeSet<String>>,
    owner: BTreeMap<String, Option<Ev>>,
    syms: BTreeMap<Ev, Vec<(String, Sym)>>,
    ran: BTreeMap<String, Option<LocalId>>,
    ctx: BTreeMap<String, LocalId>,
    scope: Vec<Ev>,
    loops: BTreeMap<String, LocalId>,
    writes: BTreeSet<String>,
}

/// A schema's IR type and bounds.
fn schema_ty(s: &SchemaDef) -> Ty {
    let ts = s.types();
    let one = |t: &str| match t {
        "INTEGER" | "LONG" => Some(Ty::Int),
        "FLOAT" | "DOUBLE" => Some(Ty::Float),
        "STRING" => Some(Ty::Str),
        "BOOLEAN" => Some(Ty::Bool),
        _ => None,
    };
    let mut acc: Option<Ty> = None;
    for t in &ts {
        let Some(x) = one(t) else { return Ty::Json };
        acc = match acc {
            None => Some(x),
            Some(a) => match a.join(x) {
                Some(j) => Some(j),
                None => return Ty::Json,
            },
        };
    }
    acc.unwrap_or(Ty::Json)
}

fn json_lit(v: &serde_json::Value) -> Expr {
    use serde_json::Value as J;
    Expr::Lit(match v {
        J::Null => Literal::Null,
        J::Bool(b) => Literal::Bool(*b),
        J::Number(n) => n
            .as_i64()
            .map_or_else(|| Literal::Float(n.as_f64().unwrap_or(0.0)), Literal::Int),
        J::String(s) => Literal::Str(s.clone()),
        other => Literal::Json(other.clone()),
    })
}

/// `Steps.<step>.<event>[...]` → (step, event).
fn parse_dep_key(k: &str) -> Option<Ev> {
    let rest = k.strip_prefix("Steps.")?;
    let mut it = rest.splitn(3, '.');
    let s = it.next()?;
    let e = it.next()?;
    (!s.is_empty() && !e.is_empty()).then(|| (s.to_owned(), e.to_owned()))
}

fn parse_expr(text: &str, at: &str) -> Result<KExpr> {
    expr::parse(text).map_err(|e| syntax(format!("{at}: can't parse {text:?}: {e}")))
}

impl L<'_> {
    fn reason(&mut self, code: &str, at: &str, detail: impl Into<String>) {
        self.reasons.push(Reason::new(code, at, detail));
    }

    fn step(&self, name: &str) -> &Step {
        &self.def.steps[name]
    }

    // -- signature ---------------------------------------------------------

    fn signature(&mut self) {
        for (name, p) in &self.def.parameters.0 {
            let ty = if p.variable_argument {
                Ty::Json
            } else {
                schema_ty(&p.schema)
            };
            self.f.params.push(Param {
                name: name.clone(),
                ty,
                min: p.schema.minimum,
                max: p.schema.maximum,
            });
        }
        let mut events: Vec<EventDef> = self
            .def
            .events
            .0
            .iter()
            .map(|(name, e)| EventDef {
                name: name.clone(),
                fields: e
                    .parameters
                    .0
                    .iter()
                    .map(|(n, s)| (n.clone(), schema_ty(s)))
                    .collect(),
            })
            .collect();
        events.sort_by_key(|e| e.name != "output");
        self.f.events = events;
    }

    // -- the graph ---------------------------------------------------------

    fn graph(&mut self) -> Result<()> {
        let def = self.def;
        for (name, step) in &def.steps {
            if !step.statement_name.is_empty() && step.statement_name != *name {
                return Err(syntax(format!(
                    "step {name}: statementName {:?} must equal its key",
                    step.statement_name
                )));
            }
            let at = format!("step {name}");
            let mut d = BTreeSet::new();
            for (k, v) in &step.dependent_statements {
                if *v {
                    d.insert(parse_dep_key(k).ok_or_else(|| {
                        syntax(format!(
                            "{at}: dependency {k:?} is not Steps.<step>.<event>"
                        ))
                    })?);
                }
            }
            for (k, v) in &step.execute_iftrue {
                if *v {
                    d.extend(parse_expr(k, &at)?.step_refs());
                }
            }
            for (pname, refs) in &step.parameter_map {
                for r in refs.values() {
                    for e in ref_exprs(step, pname, r) {
                        d.extend(parse_expr(&e, &format!("{at}, parameter {pname}"))?.step_refs());
                    }
                }
            }
            for (s, _) in &d {
                if !def.steps.contains_key(s) {
                    return Err(syntax(format!("{at} reads step {s}, which doesn't exist")));
                }
                if s == name {
                    return Err(syntax(format!("{at} depends on itself")));
                }
            }
            self.deps.insert(name.clone(), d);
        }
        // Cycles.
        let mut indeg: BTreeMap<&str, usize> = def.steps.keys().map(|k| (k.as_str(), 0)).collect();
        let mut succ: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (s, d) in &self.deps {
            let preds: BTreeSet<&str> = d.iter().map(|(t, _)| t.as_str()).collect();
            for t in preds {
                succ.entry(t).or_default().insert(s.as_str());
                *indeg.entry(s.as_str()).or_default() += 1;
            }
        }
        let mut q: VecDeque<&str> = indeg
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(k, _)| *k)
            .collect();
        let mut seen = 0;
        while let Some(s) = q.pop_front() {
            seen += 1;
            for t in succ.get(s).into_iter().flatten() {
                let n = indeg.get_mut(t).map_or(0, |n| {
                    *n -= 1;
                    *n
                });
                if n == 0 {
                    q.push_back(t);
                }
            }
        }
        if seen != def.steps.len() {
            let cyc: Vec<&str> = indeg
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(k, _)| *k)
                .collect();
            return Err(syntax(format!("steps form a cycle: {}", cyc.join(", "))));
        }
        // Branch sub-graphs.
        let mut out: BTreeMap<&str, BTreeMap<&str, BTreeSet<&str>>> = BTreeMap::new();
        for (s, d) in &self.deps {
            for (t, e) in d {
                out.entry(t.as_str())
                    .or_default()
                    .entry(e.as_str())
                    .or_default()
                    .insert(s.as_str());
            }
        }
        let mut branch = BTreeMap::new();
        for (c, evs) in &out {
            for (e, direct) in evs {
                if *e == "output" {
                    continue;
                }
                let mut set: BTreeSet<String> = BTreeSet::new();
                let mut q: VecDeque<&str> = direct.iter().copied().collect();
                while let Some(x) = q.pop_front() {
                    if !set.insert(x.to_owned()) {
                        continue;
                    }
                    for nexts in out.get(x).into_iter().flat_map(BTreeMap::values) {
                        q.extend(nexts.iter().copied());
                    }
                }
                branch.insert(((*c).to_owned(), (*e).to_owned()), set);
            }
        }
        for (c, e) in branch.keys() {
            let f = self.step(c).function();
            let ok = match f.as_str() {
                IF => e == "true" || e == "false",
                RANGE | COUNT | FOREACH => e == "iteration",
                _ => false,
            };
            if !ok {
                self.reason(
                    "kirun.event",
                    &format!("step {c}"),
                    format!("steps wait for its {e} event, which has no compiled form"),
                );
            }
        }
        self.branch = branch;
        // Owners: the smallest branch holding each step; the sets must nest.
        for s in def.steps.keys() {
            let mut holding: Vec<(&Ev, usize)> = self
                .branch
                .iter()
                .filter(|(_, set)| set.contains(s))
                .map(|(k, set)| (k, set.len()))
                .collect();
            holding.sort_by_key(|(_, n)| *n);
            for w in holding.windows(2) {
                if !self.branch[w[0].0].is_subset(&self.branch[w[1].0]) {
                    self.reasons.push(Reason::new(
                        "kirun.tangled_graph",
                        &format!("step {s}"),
                        format!(
                            "it waits for both Steps.{}.{} and Steps.{}.{}",
                            w[0].0.0, w[0].0.1, w[1].0.0, w[1].0.1
                        ),
                    ));
                }
            }
            for ((c, e), _) in &holding {
                if self.deps[s].iter().any(|(t, e2)| t == c && e2 != e) {
                    self.reasons.push(Reason::new(
                        "kirun.tangled_graph",
                        &format!("step {s}"),
                        format!("it runs on Steps.{c}.{e} and waits for another event of {c}"),
                    ));
                }
            }
            self.owner
                .insert(s.clone(), holding.first().map(|(k, _)| (*k).clone()));
        }
        Ok(())
    }

    /// The step `t` stands for in `scope`: itself, or the control step of
    /// `scope` it sits under; `None` when it is outside `scope`.
    fn rep(&self, t: &str, scope: Option<&Ev>) -> Option<String> {
        let mut cur = t.to_owned();
        for _ in 0..=self.def.steps.len() {
            let o = self.owner.get(&cur).cloned().flatten();
            if o.as_ref() == scope {
                return Some(cur);
            }
            cur = o?.0;
        }
        None
    }

    /// `s` and every step in its branches.
    fn subtree(&self, s: &str) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = BTreeSet::from([s.to_owned()]);
        for ((c, _), set) in &self.branch {
            if c == s {
                out.extend(set.iter().cloned());
            }
        }
        out
    }

    fn writes(&self, s: &str) -> bool {
        self.subtree(s)
            .iter()
            .any(|t| self.step(t).function() == "Sprig.Db.Exec")
    }

    // -- scopes ------------------------------------------------------------

    #[allow(clippy::needless_pass_by_value)]
    fn scope_block(&mut self, scope: Option<Ev>) -> Result<Vec<Stmt>> {
        let members: Vec<String> = self
            .def
            .steps
            .keys()
            .filter(|s| self.owner.get(*s).cloned().flatten() == scope)
            .cloned()
            .collect();
        let mut preds: BTreeMap<String, BTreeSet<String>> = members
            .iter()
            .map(|m| (m.clone(), BTreeSet::new()))
            .collect();
        for s in &members {
            for t in self.subtree(s) {
                for (d, _) in &self.deps[&t] {
                    if let Some(r) = self.rep(d, scope.as_ref())
                        && r != *s
                        && preds.contains_key(&r)
                    {
                        preds.get_mut(s).map(|p| p.insert(r));
                    }
                }
            }
        }
        let mut order = Vec::new();
        let mut done: BTreeSet<String> = BTreeSet::new();
        while done.len() < members.len() {
            let ready: Vec<String> = members
                .iter()
                .filter(|m| !done.contains(*m) && preds[*m].iter().all(|p| done.contains(p)))
                .cloned()
                .collect();
            if ready.is_empty() {
                let left: Vec<&String> = members.iter().filter(|m| !done.contains(*m)).collect();
                self.reason(
                    "kirun.order",
                    "definition",
                    format!(
                        "steps {} wait on each other across a branch",
                        left.iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
                break;
            }
            let w: Vec<&String> = ready.iter().filter(|s| self.writes(s)).collect();
            if w.len() > 1 {
                self.lints.push(Reason::new(
                    "kirun.unordered_writes",
                    &format!(
                        "steps {}",
                        w.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                    ),
                    "KIRun may run these writes in any order; compiled code runs them by name",
                ));
            }
            // One at a time, by name, so later ready sets see this one done.
            let first = ready[0].clone();
            done.insert(first.clone());
            order.push(first);
        }
        if let Some(sc) = &scope {
            self.scope.push(sc.clone());
        }
        let mut block = Vec::new();
        for s in order {
            let stmts = self.guarded(&s)?;
            block.extend(stmts);
        }
        if scope.is_some() {
            self.scope.pop();
        }
        Ok(block)
    }

    fn ran_flag(&self, step: &str) -> Option<LocalId> {
        self.ran.get(step).copied().flatten()
    }

    fn guarded(&mut self, s: &str) -> Result<Vec<Stmt>> {
        let at = format!("step {s}");
        let mut guards: Vec<Expr> = Vec::new();
        let deps = self.deps[s].clone();
        for (d, e) in &deps {
            if self.scope.iter().any(|(c, ev)| c == d && ev == e) {
                continue;
            }
            if e == "output"
                && let Some(flag) = self.ran_flag(d)
            {
                guards.push(Expr::Local(flag));
            }
        }
        let step = self.step(s).clone();
        for (k, v) in &step.execute_iftrue {
            if !*v {
                continue;
            }
            let ke = parse_expr(k, &at)?;
            if let Some(p) = ke.path()
                && p.len() == 3
                && p[0] == "Steps"
            {
                let (c, ev) = (p[1], p[2]);
                if self.scope.iter().any(|(sc, se)| sc == c && se == ev) {
                    continue;
                }
                if ev == "output" {
                    if let Some(flag) = self.ran_flag(c) {
                        guards.push(Expr::Local(flag));
                    }
                    continue;
                }
                guards.push(Expr::bool(false));
                continue;
            }
            let e = self.ex(&ke, &at);
            guards.push(Expr::call(Builtin::Present, vec![e]));
        }
        let mut body = self.lower_step(s)?;
        if guards.is_empty() {
            self.ran.insert(s.to_owned(), None);
            return Ok(body);
        }
        let flag = self.f.local(&format!("ran_{s}"), Ty::Bool);
        self.ran.insert(s.to_owned(), Some(flag));
        let cond = guards
            .into_iter()
            .reduce(|a, b| Expr::bin(BinOp::And, a, b))
            .unwrap_or(Expr::bool(true));
        body.push(Stmt::Assign(flag, Expr::bool(true)));
        Ok(vec![Stmt::If(
            cond,
            body,
            vec![Stmt::Assign(flag, Expr::bool(false))],
        )])
    }

    // -- parameters ----------------------------------------------------------

    /// The single value bound to `param`, or `None` when unbound.
    fn arg(&mut self, s: &str, param: &str) -> Result<Option<Expr>> {
        let step = self.step(s).clone();
        let refs = step.refs(param);
        let Some(r) = refs.first() else {
            return Ok(None);
        };
        self.ref_value(s, param, r).map(Some)
    }

    /// Every value bound to a variable-argument `param`, in order.
    fn var_args(&mut self, s: &str, param: &str) -> Result<Vec<Expr>> {
        let step = self.step(s).clone();
        let mut out = Vec::new();
        for r in step.refs(param) {
            out.push(self.ref_value(s, param, r)?);
        }
        Ok(out)
    }

    fn ref_value(&mut self, s: &str, param: &str, r: &ParamRef) -> Result<Expr> {
        let at = format!("step {s}, parameter {param}");
        match r.kind {
            RefKind::Value => Ok(r.value.as_ref().map_or(Expr::Lit(Literal::Null), json_lit)),
            RefKind::Expression => {
                let text = r.expression.clone().unwrap_or_default();
                if text.trim().is_empty() {
                    return Ok(Expr::Lit(Literal::Null));
                }
                let k = parse_expr(&text, &at)?;
                Ok(self.ex(&k, &at))
            }
        }
    }

    /// A constant string parameter.
    fn const_str(&self, s: &str, param: &str) -> Option<String> {
        let step = self.step(s);
        let r = step.refs(param).into_iter().next()?;
        match (&r.kind, &r.value) {
            (RefKind::Value, Some(serde_json::Value::String(v))) => Some(v.clone()),
            _ => None,
        }
    }

    // -- steps ---------------------------------------------------------------

    fn set_syms(&mut self, s: &str, ev: &str, syms: Vec<(String, Sym)>) {
        self.syms.insert((s.to_owned(), ev.to_owned()), syms);
    }

    #[allow(clippy::too_many_lines)]
    fn lower_step(&mut self, s: &str) -> Result<Vec<Stmt>> {
        let at = format!("step {s}");
        let func = self.step(s).function();
        let mut out = Vec::new();
        match func.as_str() {
            "System.GenerateEvent" => {
                let name = match self.arg(s, "eventName")? {
                    None => "output".to_owned(),
                    Some(Expr::Lit(Literal::Str(n))) => n,
                    Some(_) => {
                        self.reason("kirun.event_name", &at, "the event name must be a constant");
                        "output".to_owned()
                    }
                };
                let step = self.step(s).clone();
                let mut fields = Vec::new();
                for r in step.refs("results") {
                    let (fname, value) = self.result_entry(s, r)?;
                    fields.push((fname, value));
                }
                let mut typed = Vec::new();
                let declared = self.f.event(&name).cloned();
                for (fname, v) in fields {
                    let want = declared
                        .as_ref()
                        .and_then(|d| d.fields.iter().find(|(n, _)| *n == fname))
                        .map(|(_, t)| *t);
                    let v = match want {
                        Some(t) => self.coerce(v, t, &at),
                        None => v,
                    };
                    typed.push((fname, v));
                }
                if declared.is_none() {
                    let fields = typed
                        .iter()
                        .map(|(n, e)| (n.clone(), self.f.ty_of(e)))
                        .collect();
                    self.f.events.push(EventDef {
                        name: name.clone(),
                        fields,
                    });
                }
                out.push(Stmt::Emit(name, typed));
                self.set_syms(s, "output", Vec::new());
            }
            IF => {
                let c = self
                    .arg(s, "condition")?
                    .unwrap_or(Expr::Lit(Literal::Null));
                let t = self.scope_block(Some((s.to_owned(), "true".to_owned())))?;
                let e = self.scope_block(Some((s.to_owned(), "false".to_owned())))?;
                out.push(Stmt::If(Expr::call(Builtin::Truthy, vec![c]), t, e));
                for ev in ["true", "false", "output"] {
                    self.set_syms(s, ev, Vec::new());
                }
            }
            RANGE | COUNT | FOREACH => out.extend(self.lower_loop(s, &func)?),
            "System.Loop.Break" => {
                let target = self.const_str(s, "stepName");
                match target.and_then(|t| self.loops.get(&t).copied()) {
                    Some(flag) => out.push(Stmt::Assign(flag, Expr::bool(true))),
                    None => self.reason(
                        "kirun.break_target",
                        &at,
                        "Break must name an enclosing loop as a constant",
                    ),
                }
                self.set_syms(s, "output", Vec::new());
            }
            "System.Context.Create" => {
                let Some(name) = self.const_str(s, "name") else {
                    return Err(syntax(format!(
                        "{at}: Context.Create needs a constant name"
                    )));
                };
                let step = self.step(s).clone();
                let schema: SchemaDef = step
                    .refs("schema")
                    .first()
                    .and_then(|r| r.value.clone())
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default();
                let ty = schema_ty(&schema);
                let local = match self.ctx.get(&name) {
                    Some(l) => *l,
                    None => {
                        let l = self.f.local(&format!("ctx_{name}"), ty);
                        self.ctx.insert(name.clone(), l);
                        l
                    }
                };
                let init = schema
                    .default_value
                    .as_ref()
                    .map_or(Expr::Lit(Literal::Null), json_lit);
                let init = self.coerce(init, ty, &at);
                out.push(Stmt::Assign(local, init));
                self.set_syms(s, "output", Vec::new());
            }
            "System.Context.Set" => {
                let name = self.const_str(s, "name");
                let value = self.arg(s, "value")?.unwrap_or(Expr::Lit(Literal::Null));
                let key = name.as_deref().and_then(|n| n.strip_prefix("Context."));
                match key {
                    Some(k) if !k.contains(['.', '[']) => {
                        let local = match self.ctx.get(k) {
                            Some(l) => *l,
                            None => {
                                let ty = self.f.ty_of(&value);
                                let ty = if ty == Ty::Null { Ty::Json } else { ty };
                                let l = self.f.local(&format!("ctx_{k}"), ty);
                                self.ctx.insert(k.to_owned(), l);
                                l
                            }
                        };
                        let ty = self.f.local_def(local).ty;
                        let v = self.coerce(value, ty, &at);
                        out.push(Stmt::Assign(local, v));
                    }
                    _ => self.reason(
                        "kirun.context_path",
                        &at,
                        "Context.Set compiles only for a constant Context.<name>",
                    ),
                }
                self.set_syms(s, "output", Vec::new());
            }
            "System.Context.Get" => {
                let name = self.const_str(s, "name");
                let v = match name.as_ref().and_then(|n| self.ctx.get(n)) {
                    Some(l) => Expr::Local(*l),
                    None => {
                        self.reason("kirun.context_path", &at, "Context.Get of an unknown name");
                        Expr::Lit(Literal::Null)
                    }
                };
                self.set_syms(s, "output", vec![("value".to_owned(), Sym::E(v))]);
            }
            "System.Math.Add" | "System.Math.Maximum" | "System.Math.Minimum" => {
                let args = self.var_args(s, "value")?;
                let args: Vec<Expr> = args.into_iter().map(|a| self.number(a, &at)).collect();
                let v = match func.as_str() {
                    "System.Math.Add" => args
                        .into_iter()
                        .reduce(|a, b| Expr::bin(BinOp::Add, a, b))
                        .unwrap_or(Expr::int(0)),
                    "System.Math.Maximum" => Expr::call(Builtin::Max, args),
                    _ => Expr::call(Builtin::Min, args),
                };
                out.extend(self.result_local(s, "value", v));
            }
            "System.Math.Absolute"
            | "System.Math.Floor"
            | "System.Math.Ceiling"
            | "System.Math.Round"
            | "System.Math.SquareRoot" => {
                let a = self.arg(s, "value")?.unwrap_or(Expr::Lit(Literal::Null));
                let a = self.number(a, &at);
                let b = match func.as_str() {
                    "System.Math.Absolute" => Builtin::Abs,
                    "System.Math.Floor" => Builtin::Floor,
                    "System.Math.Ceiling" => Builtin::Ceil,
                    "System.Math.Round" => Builtin::Round,
                    _ => Builtin::Sqrt,
                };
                out.extend(self.result_local(s, "value", Expr::call(b, vec![a])));
            }
            "System.Math.Power" => {
                let a = self.arg(s, "value1")?.unwrap_or(Expr::Lit(Literal::Null));
                let b = self.arg(s, "value2")?.unwrap_or(Expr::Lit(Literal::Null));
                let (a, b) = (self.number(a, &at), self.number(b, &at));
                out.extend(self.result_local(s, "value", Expr::call(Builtin::Pow, vec![a, b])));
            }
            "System.String.Concatenate" => {
                let args = self.var_args(s, "value")?;
                let mut acc: Option<Expr> = None;
                for a in args {
                    let a = self.text(a, &at);
                    acc = Some(match acc {
                        None => a,
                        Some(x) => Expr::bin(BinOp::Concat, x, a),
                    });
                }
                let v = acc.unwrap_or(Expr::Lit(Literal::Str(String::new())));
                out.extend(self.result_local(s, "value", v));
            }
            "System.String.Length" | "System.String.UpperCase" | "System.String.LowerCase" => {
                let a = self.arg(s, "string")?.unwrap_or(Expr::Lit(Literal::Null));
                let a = self.text(a, &at);
                let b = match func.as_str() {
                    "System.String.Length" => Builtin::Len,
                    "System.String.UpperCase" => Builtin::Upper,
                    _ => Builtin::Lower,
                };
                out.extend(self.result_local(s, "result", Expr::call(b, vec![a])));
            }
            "System.String.Contains"
            | "System.String.StartsWith"
            | "System.String.EndsWith"
            | "System.String.IndexOf" => {
                let a = self.arg(s, "string")?.unwrap_or(Expr::Lit(Literal::Null));
                let b = self
                    .arg(s, "searchString")?
                    .unwrap_or(Expr::Lit(Literal::Null));
                let (a, b) = (self.text(a, &at), self.text(b, &at));
                let f = match func.as_str() {
                    "System.String.Contains" => Builtin::Contains,
                    "System.String.StartsWith" => Builtin::StartsWith,
                    "System.String.EndsWith" => Builtin::EndsWith,
                    _ => Builtin::IndexOf,
                };
                out.extend(self.result_local(s, "result", Expr::call(f, vec![a, b])));
            }
            "Sprig.Db.QueryOne" | "Sprig.Db.Query" | "Sprig.Db.Exec" => {
                out.extend(self.lower_db(s, &func)?);
            }
            other => {
                self.reason(
                    "kirun.function",
                    &at,
                    format!("{other} has no compiled form"),
                );
            }
        }
        Ok(out)
    }

    /// `{name, value: {isExpression, value}}` of a `GenerateEvent` result.
    fn result_entry(&mut self, s: &str, r: &ParamRef) -> Result<(String, Expr)> {
        let at = format!("step {s}, parameter results");
        if r.kind != RefKind::Value {
            self.reason(
                "kirun.results",
                &at,
                "results must be constants of {name, value}",
            );
            return Ok((String::new(), Expr::Lit(Literal::Null)));
        }
        let v = r.value.clone().unwrap_or_default();
        let name = v
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| syntax(format!("{at}: a result without a name")))?
            .to_owned();
        let inner = v
            .get("value")
            .ok_or_else(|| syntax(format!("{at}: result {name} has no value")))?;
        let is_expr = inner
            .get("isExpression")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let raw = inner
            .get("value")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let e = if is_expr {
            let text = raw.as_str().unwrap_or_default().to_owned();
            let k = parse_expr(&text, &at)?;
            self.ex(&k, &at)
        } else {
            json_lit(&raw)
        };
        Ok((name, e))
    }

    /// Assigns `v` to a new local and exposes it as `output.<field>`.
    fn result_local(&mut self, s: &str, field: &str, v: Expr) -> Vec<Stmt> {
        let ty = self.f.ty_of(&v);
        let ty = if ty == Ty::Null { Ty::Json } else { ty };
        let l = self.f.local(&format!("{s}_{field}"), ty);
        self.set_syms(
            s,
            "output",
            vec![(field.to_owned(), Sym::E(Expr::Local(l)))],
        );
        vec![Stmt::Assign(l, v)]
    }

    fn lower_loop(&mut self, s: &str, func: &str) -> Result<Vec<Stmt>> {
        let at = format!("step {s}");
        let mut out = Vec::new();
        let brk = self.f.local(&format!("{s}_break"), Ty::Bool);
        self.loops.insert(s.to_owned(), brk);
        let (cur, cond, step_by, bound, each): (
            LocalId,
            Expr,
            Expr,
            Bound,
            Option<(LocalId, Expr)>,
        ) = match func {
            RANGE => {
                let from = self.arg(s, "from")?.unwrap_or(Expr::int(0));
                let to = self.arg(s, "to")?.unwrap_or(Expr::int(1));
                let step = self.arg(s, "step")?.unwrap_or(Expr::int(1));
                let (from, to) = (self.number(from, &at), self.number(to, &at));
                let stepv = match &step {
                    Expr::Lit(Literal::Int(v)) => Some(*v as f64),
                    Expr::Lit(Literal::Float(v)) => Some(*v),
                    _ => None,
                };
                let Some(stepv) = stepv else {
                    self.reason(
                        "kirun.loop_step",
                        &at,
                        "RangeLoop's step must be a constant",
                    );
                    return Ok(out);
                };
                if stepv == 0.0 {
                    return Err(syntax(format!("{at}: RangeLoop's step can't be 0")));
                }
                let ty = [&from, &to, &step]
                    .iter()
                    .map(|e| self.f.ty_of(e))
                    .try_fold(Ty::Null, Ty::join)
                    .unwrap_or(Ty::Float);
                let ty = if ty == Ty::Null { Ty::Int } else { ty };
                let bound = range_bound(&self.f, &from, &to, stepv);
                let cur = self.f.local(&format!("{s}_value"), ty);
                let end = self.f.local(&format!("{s}_to"), ty);
                let from = self.coerce(from, ty, &at);
                let to = self.coerce(to, ty, &at);
                let step = self.coerce(step, ty, &at);
                out.push(Stmt::Assign(cur, from));
                out.push(Stmt::Assign(end, to));
                let op = if stepv > 0.0 { BinOp::Lt } else { BinOp::Gt };
                (
                    cur,
                    Expr::bin(op, Expr::Local(cur), Expr::Local(end)),
                    step,
                    bound,
                    None,
                )
            }
            COUNT => {
                let count = self.arg(s, "count")?.unwrap_or(Expr::int(0));
                let count = self.number(count, &at);
                let bound = range_bound(&self.f, &Expr::int(0), &count, 1.0);
                let ty = self.f.ty_of(&count);
                let end_ty = if ty == Ty::Float { Ty::Float } else { Ty::Int };
                let cur = self.f.local(&format!("{s}_value"), Ty::Int);
                let end = self.f.local(&format!("{s}_count"), end_ty);
                out.push(Stmt::Assign(cur, Expr::int(0)));
                out.push(Stmt::Assign(end, count));
                (
                    cur,
                    Expr::bin(BinOp::Lt, Expr::Local(cur), Expr::Local(end)),
                    Expr::int(1),
                    bound,
                    None,
                )
            }
            _ => {
                let src = self.arg(s, "source")?.unwrap_or(Expr::Lit(Literal::Null));
                let src = self.coerce(src, Ty::Json, &at);
                let arr = self.f.local(&format!("{s}_source"), Ty::Json);
                let n = self.f.local(&format!("{s}_length"), Ty::Int);
                let cur = self.f.local(&format!("{s}_value"), Ty::Int);
                out.push(Stmt::Assign(arr, src));
                out.push(Stmt::Assign(
                    n,
                    Expr::bin(
                        BinOp::Coalesce,
                        Expr::call(Builtin::JsonLength, vec![Expr::Local(arr)]),
                        Expr::int(0),
                    ),
                ));
                out.push(Stmt::Assign(cur, Expr::int(0)));
                let each_v = Expr::Path(
                    Box::new(Expr::Local(arr)),
                    vec![PathSeg::Index(Box::new(Expr::Local(cur)))],
                );
                let each = self.f.local(&format!("{s}_each"), Ty::Json);
                (
                    cur,
                    Expr::bin(BinOp::Lt, Expr::Local(cur), Expr::Local(n)),
                    Expr::int(1),
                    Bound::Guarded,
                    Some((each, each_v)),
                )
            }
        };
        out.push(Stmt::Assign(brk, Expr::bool(false)));
        let idx = self
            .f
            .local(&format!("{s}_index"), self.f.local_def(cur).ty);
        let mut it = vec![("index".to_owned(), Sym::E(Expr::Local(idx)))];
        let mut body = vec![Stmt::Assign(idx, Expr::Local(cur))];
        if let Some((each, v)) = each {
            body.push(Stmt::Assign(each, v));
            it.push(("each".to_owned(), Sym::E(Expr::Local(each))));
        }
        self.set_syms(s, "iteration", it);
        // KIRun advances the counter before the body runs.
        body.push(Stmt::Assign(
            cur,
            Expr::bin(BinOp::Add, Expr::Local(cur), step_by),
        ));
        body.extend(self.scope_block(Some((s.to_owned(), "iteration".to_owned())))?);
        let cond = Expr::bin(
            BinOp::And,
            cond,
            Expr::Unary(UnOp::Not, Box::new(Expr::Local(brk))),
        );
        out.push(Stmt::While { cond, body, bound });
        self.set_syms(
            s,
            "output",
            vec![("value".to_owned(), Sym::E(Expr::Local(cur)))],
        );
        Ok(out)
    }

    fn lower_db(&mut self, s: &str, func: &str) -> Result<Vec<Stmt>> {
        let at = format!("step {s}");
        let Some(text) = self.const_str(s, "sql") else {
            self.reason("db.dynamic_sql", &at, "the SQL text must be a constant");
            self.set_syms(s, "output", Vec::new());
            return Ok(Vec::new());
        };
        let binds = self.var_args(s, "args")?;
        let want = if func == "Sprig.Db.Exec" {
            Want::Write
        } else {
            Want::Read
        };
        let bound = match sql::bind(&text, want, self.cat, &at) {
            Ok(b) => b,
            Err(r) => {
                self.reasons.push(r);
                let l = self.f.local(&format!("{s}_unbound"), Ty::Json);
                let field = match func {
                    "Sprig.Db.QueryOne" => "row",
                    "Sprig.Db.Query" => "rows",
                    _ => "affected",
                };
                self.set_syms(
                    s,
                    "output",
                    vec![(field.to_owned(), Sym::E(Expr::Local(l)))],
                );
                return Ok(Vec::new());
            }
        };
        if bound.binds != binds.len() {
            return Err(SprigError::new(
                SprigCode::LogicSignatureMismatch,
                format!(
                    "{at}: the SQL has {} placeholders but {} args",
                    bound.binds,
                    binds.len()
                ),
            ));
        }
        for w in &bound.stmt.writes {
            self.writes.insert(w.clone());
        }
        let binds: Vec<Expr> = binds
            .into_iter()
            .inspect(|b| {
                if self.f.ty_of(b) == Ty::Json {
                    self.reason("db.json_bind", &at, "JSON values can't be bound yet");
                }
            })
            .collect();
        let shape = match func {
            "Sprig.Db.QueryOne" => {
                let found = self.f.local(&format!("{s}_found"), Ty::Bool);
                let cols: Vec<(String, LocalId)> = bound
                    .columns
                    .iter()
                    .map(|(c, t)| (c.clone(), self.f.local(&format!("{s}_{c}"), *t)))
                    .collect();
                self.set_syms(
                    s,
                    "output",
                    vec![
                        (
                            "row".to_owned(),
                            Sym::Row {
                                found,
                                cols: cols.clone(),
                            },
                        ),
                        ("found".to_owned(), Sym::E(Expr::Local(found))),
                    ],
                );
                SqlShape::One { found, into: cols }
            }
            "Sprig.Db.Query" => {
                let into = self.f.local(&format!("{s}_rows"), Ty::Json);
                self.set_syms(
                    s,
                    "output",
                    vec![("rows".to_owned(), Sym::E(Expr::Local(into)))],
                );
                SqlShape::Many {
                    into,
                    cols: bound.columns.clone(),
                }
            }
            _ => {
                let affected = self.f.local(&format!("{s}_affected"), Ty::Int);
                self.set_syms(
                    s,
                    "output",
                    vec![("affected".to_owned(), Sym::E(Expr::Local(affected)))],
                );
                SqlShape::Exec { affected }
            }
        };
        Ok(vec![Stmt::Db(DbOp::Sql {
            stmt: bound.stmt,
            binds,
            shape,
            origin: at,
        })])
    }

    // -- expressions ---------------------------------------------------------

    fn ex(&mut self, k: &KExpr, at: &str) -> Expr {
        match self.expr(k, at) {
            Ok(e) => e,
            Err(r) => {
                self.reasons.push(r);
                Expr::Lit(Literal::Null)
            }
        }
    }

    fn number(&mut self, e: Expr, at: &str) -> Expr {
        match self.f.ty_of(&e) {
            Ty::Int | Ty::Float | Ty::Null => e,
            Ty::Json => Expr::call(Builtin::JsonNum, vec![e]),
            t => {
                self.reason(
                    "types.mismatch",
                    at,
                    format!("a number was expected, not {t}"),
                );
                e
            }
        }
    }

    fn text(&mut self, e: Expr, at: &str) -> Expr {
        match self.f.ty_of(&e) {
            Ty::Str => e,
            Ty::Int | Ty::Float | Ty::Bool | Ty::Null => Expr::call(Builtin::ToStr, vec![e]),
            Ty::Json => {
                self.reason(
                    "types.json_text",
                    at,
                    "JSON values don't convert to text yet",
                );
                e
            }
        }
    }

    fn coerce(&mut self, e: Expr, want: Ty, at: &str) -> Expr {
        let have = self.f.ty_of(&e);
        match (have, want) {
            (a, b) if a == b => e,
            (Ty::Null, _) => e,
            (Ty::Int, Ty::Float) => Expr::call(Builtin::ToFloat, vec![e]),
            (Ty::Json, Ty::Float) => Expr::call(Builtin::JsonNum, vec![e]),
            (Ty::Json, Ty::Str) => Expr::call(Builtin::JsonStr, vec![e]),
            (_, Ty::Json) => Expr::call(Builtin::ToJson, vec![e]),
            (Ty::Int | Ty::Float | Ty::Bool, Ty::Str) => Expr::call(Builtin::ToStr, vec![e]),
            (h, w) => {
                self.reason("types.mismatch", at, format!("{w} was expected, not {h}"));
                e
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn expr(&mut self, k: &KExpr, at: &str) -> std::result::Result<Expr, Reason> {
        let r = |code: &str, d: String| Reason::new(code, at, d);
        Ok(match k {
            KExpr::Num(Num::Int(v)) => Expr::int(*v),
            KExpr::Num(Num::Float(v)) => Expr::Lit(Literal::Float(*v)),
            KExpr::Str(s) => Expr::Lit(Literal::Str(s.clone())),
            KExpr::Bool(b) => Expr::bool(*b),
            KExpr::Null => Expr::Lit(Literal::Null),
            KExpr::Ident(_) | KExpr::Member(..) | KExpr::Index(..) => self.reference(k, at)?,
            KExpr::Range(..) => {
                return Err(r(
                    "kirun.range",
                    "array ranges have no compiled form".into(),
                ));
            }
            KExpr::Unary(op, a) => {
                let a = self.expr(a, at)?;
                match op {
                    KUn::Plus => self.number(a, at),
                    KUn::Minus => Expr::Unary(UnOp::Neg, Box::new(self.number(a, at))),
                    KUn::Not => Expr::Unary(UnOp::Not, Box::new(a)),
                    KUn::BitNot => return Err(r("kirun.bitwise", "~ has no compiled form".into())),
                }
            }
            KExpr::Ternary(c, a, b) => {
                let c = self.expr(c, at)?;
                let a = self.expr(a, at)?;
                let b = self.expr(b, at)?;
                let (a, b) = self.unify(a, b, at)?;
                Expr::Cond(
                    Box::new(Expr::call(Builtin::Truthy, vec![c])),
                    Box::new(a),
                    Box::new(b),
                )
            }
            KExpr::Binary(op, a, b) => {
                let a = self.expr(a, at)?;
                let b = self.expr(b, at)?;
                let (ta, tb) = (self.f.ty_of(&a), self.f.ty_of(&b));
                match op {
                    KBin::Add if ta == Ty::Str || tb == Ty::Str => {
                        let (a, b) = (self.text(a, at), self.text(b, at));
                        Expr::bin(BinOp::Concat, a, b)
                    }
                    KBin::Add | KBin::Sub | KBin::Mul | KBin::Div | KBin::IntDiv | KBin::Mod => {
                        for t in [ta, tb] {
                            if matches!(t, Ty::Str | Ty::Bool) {
                                return Err(r("types.mismatch", format!("{} on {t}", op.text())));
                            }
                        }
                        let (a, b) = (self.number(a, at), self.number(b, at));
                        let o = match op {
                            KBin::Add => BinOp::Add,
                            KBin::Sub => BinOp::Sub,
                            KBin::Mul => BinOp::Mul,
                            KBin::Div => BinOp::Div,
                            KBin::IntDiv => BinOp::IntDiv,
                            _ => BinOp::Mod,
                        };
                        Expr::bin(o, a, b)
                    }
                    KBin::Lt | KBin::Le | KBin::Gt | KBin::Ge | KBin::Eq | KBin::Ne => {
                        let (a, b) = self.comparable(a, b, at)?;
                        let o = match op {
                            KBin::Lt => BinOp::Lt,
                            KBin::Le => BinOp::Le,
                            KBin::Gt => BinOp::Gt,
                            KBin::Ge => BinOp::Ge,
                            KBin::Eq => BinOp::Eq,
                            _ => BinOp::Ne,
                        };
                        Expr::bin(o, a, b)
                    }
                    KBin::And => Expr::bin(BinOp::And, a, b),
                    KBin::Or => Expr::bin(BinOp::Or, a, b),
                    KBin::Coalesce => {
                        let (a, b) = self.unify(a, b, at)?;
                        Expr::bin(BinOp::Coalesce, a, b)
                    }
                    other => {
                        return Err(r(
                            "kirun.bitwise",
                            format!("{} has no compiled form", other.text()),
                        ));
                    }
                }
            }
        })
    }

    /// Two operands of one type, for `??` and `?:`.
    fn unify(&mut self, a: Expr, b: Expr, at: &str) -> std::result::Result<(Expr, Expr), Reason> {
        let (ta, tb) = (self.f.ty_of(&a), self.f.ty_of(&b));
        match ta.join(tb) {
            Some(t) => Ok((self.coerce(a, t, at), self.coerce(b, t, at))),
            None if ta == Ty::Json || tb == Ty::Json => {
                let other = if ta == Ty::Json { tb } else { ta };
                Ok((self.coerce(a, other, at), self.coerce(b, other, at)))
            }
            None => Err(Reason::new(
                "types.mismatch",
                at,
                format!("{ta} and {tb} don't mix"),
            )),
        }
    }

    fn comparable(
        &mut self,
        a: Expr,
        b: Expr,
        at: &str,
    ) -> std::result::Result<(Expr, Expr), Reason> {
        let (ta, tb) = (self.f.ty_of(&a), self.f.ty_of(&b));
        let ok = ta.join(tb).is_some();
        if ok {
            return Ok((a, b));
        }
        match (ta, tb) {
            (Ty::Json, t) | (t, Ty::Json) if t != Ty::Json => {
                let want = if t == Ty::Int { Ty::Float } else { t };
                Ok((self.coerce(a, want, at), self.coerce(b, want, at)))
            }
            _ => Err(Reason::new(
                "types.mixed_compare",
                at,
                format!("comparing {ta} with {tb} has no compiled form"),
            )),
        }
    }

    /// `Arguments.*`, `Steps.*` and `Context.*` with their rest.
    fn reference(&mut self, k: &KExpr, at: &str) -> std::result::Result<Expr, Reason> {
        // Flatten into root + segments.
        let mut segs: Vec<KSeg> = Vec::new();
        let mut cur = k;
        loop {
            match cur {
                KExpr::Member(b, n) => {
                    segs.push(KSeg::Name(n.clone()));
                    cur = b;
                }
                KExpr::Index(b, i) => {
                    segs.push(KSeg::Index((**i).clone()));
                    cur = b;
                }
                KExpr::Ident(n) => {
                    segs.push(KSeg::Name(n.clone()));
                    break;
                }
                _ => {
                    return Err(Reason::new(
                        "kirun.expression",
                        at,
                        "indexing a computed value",
                    ));
                }
            }
        }
        segs.reverse();
        let name = |s: &KSeg| match s {
            KSeg::Name(n) | KSeg::Index(KExpr::Str(n)) => Some(n.clone()),
            KSeg::Index(_) => None,
        };
        let root = name(&segs[0]).unwrap_or_default();
        let (base, rest) = match root.as_str() {
            "Arguments" => {
                let p = segs.get(1).and_then(name).ok_or_else(|| {
                    Reason::new("kirun.expression", at, "Arguments needs a parameter name")
                })?;
                let i = self
                    .f
                    .params
                    .iter()
                    .position(|x| x.name == p)
                    .ok_or_else(|| {
                        Reason::new("kirun.unknown_parameter", at, format!("no parameter {p}"))
                    })?;
                (
                    Expr::Param(u16::try_from(i).unwrap_or(u16::MAX)),
                    &segs[2..],
                )
            }
            "Context" => {
                let c = segs.get(1).and_then(name).unwrap_or_default();
                let l = self.ctx.get(&c).copied().ok_or_else(|| {
                    Reason::new(
                        "kirun.context_path",
                        at,
                        format!("Context.{c} isn't created before use"),
                    )
                })?;
                (Expr::Local(l), &segs[2..])
            }
            "Steps" => {
                let (Some(s), Some(e)) = (segs.get(1).and_then(name), segs.get(2).and_then(name))
                else {
                    return Err(Reason::new(
                        "kirun.expression",
                        at,
                        "Steps needs a step and an event",
                    ));
                };
                let Some(syms) = self.syms.get(&(s.clone(), e.clone())).cloned() else {
                    return Err(Reason::new(
                        "kirun.event",
                        at,
                        format!("Steps.{s}.{e} isn't available here"),
                    ));
                };
                let field = segs.get(3).and_then(name);
                match field {
                    None => {
                        let m = syms
                            .iter()
                            .map(|(n, sym)| (n.clone(), self.sym_value(sym)))
                            .collect::<Vec<_>>();
                        let m = m
                            .into_iter()
                            .map(|(n, v)| (n, self.coerce(v, Ty::Json, at)))
                            .collect();
                        (Expr::ObjectLit(m), &segs[3..])
                    }
                    Some(fname) => {
                        let Some((_, sym)) = syms.iter().find(|(n, _)| *n == fname) else {
                            return Err(Reason::new(
                                "kirun.event",
                                at,
                                format!("Steps.{s}.{e} has no {fname}"),
                            ));
                        };
                        if let Sym::Row { cols, .. } = sym
                            && let Some(col) = segs.get(4).and_then(name)
                        {
                            let l = cols
                                .iter()
                                .find(|(c, _)| c.eq_ignore_ascii_case(&col))
                                .map(|(_, l)| *l)
                                .ok_or_else(|| {
                                    Reason::new(
                                        "kirun.event",
                                        at,
                                        format!("the row has no column {col}"),
                                    )
                                })?;
                            (Expr::Local(l), &segs[5..])
                        } else {
                            (self.sym_value(sym), &segs[4..])
                        }
                    }
                }
            }
            other => {
                return Err(Reason::new(
                    "kirun.expression",
                    at,
                    format!("{other} isn't a value KIRun knows"),
                ));
            }
        };
        self.rest(base, rest, at)
    }

    fn sym_value(&mut self, sym: &Sym) -> Expr {
        match sym {
            Sym::E(e) => e.clone(),
            Sym::Row { found, cols } => {
                let at = "row";
                let m: Vec<(String, Expr)> = cols
                    .iter()
                    .map(|(c, l)| (c.clone(), self.coerce(Expr::Local(*l), Ty::Json, at)))
                    .collect();
                Expr::Cond(
                    Box::new(Expr::Local(*found)),
                    Box::new(Expr::ObjectLit(m)),
                    Box::new(Expr::Lit(Literal::Null)),
                )
            }
        }
    }

    fn rest(&mut self, base: Expr, rest: &[KSeg], at: &str) -> std::result::Result<Expr, Reason> {
        if rest.is_empty() {
            return Ok(base);
        }
        let ty = self.f.ty_of(&base);
        let last_is_length = matches!(rest.last(), Some(KSeg::Name(n)) if n == "length");
        match ty {
            Ty::Str if rest.len() == 1 && last_is_length => {
                Ok(Expr::call(Builtin::Len, vec![base]))
            }
            Ty::Json => {
                let (path, length) = if last_is_length {
                    (&rest[..rest.len() - 1], true)
                } else {
                    (rest, false)
                };
                let mut segs = Vec::new();
                for s in path {
                    segs.push(match s {
                        KSeg::Name(n) | KSeg::Index(KExpr::Str(n)) => PathSeg::Key(n.clone()),
                        KSeg::Index(i) => {
                            let e = self.expr(i, at)?;
                            match self.f.ty_of(&e) {
                                Ty::Int => PathSeg::Index(Box::new(e)),
                                t => {
                                    return Err(Reason::new(
                                        "kirun.expression",
                                        at,
                                        format!("an index must be a whole number, not {t}"),
                                    ));
                                }
                            }
                        }
                    });
                }
                let v = if segs.is_empty() {
                    base
                } else {
                    Expr::Path(Box::new(base), segs)
                };
                Ok(if length {
                    Expr::call(Builtin::JsonLength, vec![v])
                } else {
                    v
                })
            }
            t => Err(Reason::new(
                "kirun.expression",
                at,
                format!("a {t} value has no members"),
            )),
        }
    }
}

enum KSeg {
    Name(String),
    Index(KExpr),
}

/// The expressions inside one parameter reference: an `EXPRESSION`'s
/// text, or the `isExpression` values of `GenerateEvent`'s results.
fn ref_exprs(step: &Step, param: &str, r: &ParamRef) -> Vec<String> {
    match r.kind {
        RefKind::Expression => r
            .expression
            .iter()
            .filter(|e| !e.trim().is_empty())
            .cloned()
            .collect(),
        RefKind::Value => {
            if step.function() == "System.GenerateEvent" && param == "results" {
                let inner = r.value.as_ref().and_then(|v| v.get("value"));
                if let Some(i) = inner
                    && i.get("isExpression").and_then(serde_json::Value::as_bool) == Some(true)
                    && let Some(t) = i.get("value").and_then(|v| v.as_str())
                {
                    return vec![t.to_owned()];
                }
            }
            Vec::new()
        }
    }
}

/// A loop's static bound: constant ends, or a parameter end with a
/// `maximum`.
fn range_bound(f: &Function, from: &Expr, to: &Expr, step: f64) -> Bound {
    fn val(f: &Function, e: &Expr, upper: bool) -> Option<f64> {
        match e {
            Expr::Lit(Literal::Int(v)) => Some(*v as f64),
            Expr::Lit(Literal::Float(v)) => Some(*v),
            Expr::Param(i) => {
                let p = f.params.get(*i as usize)?;
                if upper { p.max } else { p.min }
            }
            // `Arguments.n + 1` and the like.
            Expr::Binary(op @ (BinOp::Add | BinOp::Sub), a, b) => {
                let k = match b.as_ref() {
                    Expr::Lit(Literal::Int(v)) => *v as f64,
                    Expr::Lit(Literal::Float(v)) => *v,
                    _ => return None,
                };
                let x = val(f, a, upper)?;
                Some(if *op == BinOp::Add { x + k } else { x - k })
            }
            _ => None,
        }
    }
    let (lo, hi) = if step > 0.0 {
        (val(f, from, false), val(f, to, true))
    } else {
        (val(f, to, false), val(f, from, true))
    };
    match (lo, hi) {
        (Some(a), Some(b)) => {
            let n = ((b - a) / step.abs()).ceil().max(0.0);
            if n.is_finite() && n < 1e15 {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                Bound::Static(n as u64)
            } else {
                Bound::Guarded
            }
        }
        _ => Bound::Guarded,
    }
}
