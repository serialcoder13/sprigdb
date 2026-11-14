//! Late property reads.
//!
//! In a read-only statement on a store that allows it
//! ([`GraphStore::late_props`]), hops read links and far nodes without
//! their properties (ids, routing hashes and labels only): a `MATCH` that
//! walks through thousands of messages to keep twenty no longer reads,
//! sends and parses every message's text. Before a clause evaluates
//! expressions over its rows, [`Exec::late`] reads what they need, for
//! all rows at once:
//!
//! - `v.k` needs key `k` of the element bound to `v` (or of each element
//!   of a list bound to `v`), read with only the keys needed;
//! - any other use of `v` that could look at its properties
//!   (`properties(v)`, `v {.*}`, `toString(v)`, an element inside a map or
//!   path) needs all of them;
//! - uses that only pass the element on or compare it (equality and order
//!   are by id), or read its id, labels or type, need nothing.
//!
//! The rows a statement returns are read in full ([`Exec::late_rows`]).
//! An element read late carries the keys read ([`crate::value::Partial`]);
//! reading another key is an internal error, never a wrong `null`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use sprig_common::Result;

use super::Exec;
use crate::ast::{BinOp, Expr, MapItem};
use crate::eval::Row;
use crate::store::{EdgeRow, NodeRow};
use crate::value::{Node, Rel, Value};

/// A need's pseudo-key: the node's labels (`labels(v)`, `v:L`, a label
/// in a later pattern on bound `v`).
pub(crate) const LABELS: &str = "\u{1}labels";

/// In an element's [`crate::value::Partial`] keys: its labels weren't
/// read either (a node a hop bound without reading it, because nothing
/// was known to need it).
pub(crate) const NO_LABELS: &str = "\u{1}nolabels";

fn real(k: &str) -> bool {
    !k.starts_with('\u{1}')
}

/// What a clause needs of a variable's elements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Need {
    /// Every property.
    All,
    /// These keys.
    Keys(BTreeSet<String>),
}

impl Need {
    fn merge(&mut self, other: Need) {
        match (self, other) {
            (Need::All, _) => {}
            (me @ Need::Keys(_), Need::All) => *me = Need::All,
            (Need::Keys(a), Need::Keys(b)) => a.extend(b),
        }
    }
}

/// Functions that read no property of their arguments: they pass them on
/// (`collect`, `head`), or read ids, labels or types.
/// Of those, the ones whose value is (part of) their argument.
const FORWARDING: &[&str] = &[
    "collect",
    "head",
    "last",
    "tail",
    "reverse",
    "coalesce",
    "min",
    "max",
    "nodes",
    "relationships",
];

const PASSING: &[&str] = &[
    "collect",
    "head",
    "last",
    "tail",
    "reverse",
    "coalesce",
    "count",
    "min",
    "max",
    "size",
    "length",
    "nodes",
    "relationships",
    "id",
    "elementid",
    "labels",
    "type",
    "startnode",
    "endnode",
];

pub(crate) fn add(out: &mut HashMap<String, Need>, v: &str, n: Need) {
    match out.get_mut(v) {
        Some(have) => have.merge(n),
        None => {
            out.insert(v.to_owned(), n);
        }
    }
}

/// The properties `e` reads of the variables it names. `pass`: the value
/// of `e` is only passed on or compared (a bare variable there needs
/// nothing).
pub(crate) fn needs(e: &Expr, pass: bool, out: &mut HashMap<String, Need>) {
    match e {
        Expr::Var(v) => {
            if !pass {
                add(out, v, Need::All);
            }
        }
        Expr::Prop(b, k) => match &**b {
            Expr::Var(v) => add(out, v, Need::Keys(BTreeSet::from([k.clone()]))),
            other => needs(other, false, out),
        },
        Expr::Binary(op, a, b) => {
            let cmp = matches!(
                op,
                BinOp::Eq
                    | BinOp::Ne
                    | BinOp::Lt
                    | BinOp::Le
                    | BinOp::Gt
                    | BinOp::Ge
                    | BinOp::In
                    | BinOp::And
                    | BinOp::Or
                    | BinOp::Xor
            );
            let p = cmp || (pass && matches!(op, BinOp::Add));
            needs(a, p, out);
            needs(b, p, out);
        }
        Expr::IsNull(a, _) => needs(a, true, out),
        Expr::HasLabels(a, _) => match &**a {
            Expr::Var(v) => add(out, v, Need::Keys(BTreeSet::from([LABELS.to_owned()]))),
            other => needs(other, false, out),
        },
        Expr::Func { name, args, .. }
            if name == "labels" && matches!(args.as_slice(), [Expr::Var(_)]) =>
        {
            if let [Expr::Var(v)] = args.as_slice() {
                add(out, v, Need::Keys(BTreeSet::from([LABELS.to_owned()])));
            }
        }
        Expr::Func { name, args, .. } => {
            // `collect`, `head` and the like hand their argument on (as
            // much as the call's own value is); the others read ids,
            // labels, types or sizes only.
            let p = if FORWARDING.contains(&name.as_str()) {
                pass
            } else {
                PASSING.contains(&name.as_str())
            };
            for a in args {
                needs(a, p, out);
            }
        }
        Expr::List(l) => {
            for x in l {
                needs(x, pass, out);
            }
        }
        Expr::Map(m) => {
            for (_, x) in m {
                needs(x, pass, out);
            }
        }
        Expr::Case {
            subject,
            whens,
            else_,
        } => {
            if let Some(s) = subject {
                needs(s, true, out);
            }
            for (c, r) in whens {
                // A condition or compared value: truth and equality
                // read no property.
                needs(c, true, out);
                needs(r, pass, out);
            }
            if let Some(x) = else_ {
                needs(x, pass, out);
            }
        }
        Expr::Slice(a, b, c) => {
            needs(a, pass, out);
            for x in [b, c].into_iter().flatten() {
                needs(x, false, out);
            }
        }
        Expr::Index(a, b) => {
            needs(a, false, out);
            needs(b, false, out);
        }
        Expr::Unary(_, a) => needs(a, false, out),
        Expr::ListComp {
            var,
            list,
            pred,
            map,
        } => {
            let mut inner = HashMap::new();
            if let Some(p) = pred {
                needs(p, true, &mut inner);
            }
            if let Some(m) = map {
                needs(m, pass, &mut inner);
            }
            local(var, list, inner, out);
        }
        Expr::Quantifier {
            var, list, pred, ..
        } => {
            let mut inner = HashMap::new();
            needs(pred, true, &mut inner);
            local(var, list, inner, out);
        }
        Expr::Reduce {
            acc,
            init,
            var,
            list,
            expr,
        } => {
            needs(init, false, out);
            let mut inner = HashMap::new();
            needs(expr, false, &mut inner);
            inner.remove(acc);
            local(var, list, inner, out);
        }
        Expr::MapProjection(b, items) => {
            if let Expr::Var(v) = &**b {
                for it in items {
                    match it {
                        MapItem::All => add(out, v, Need::All),
                        MapItem::Prop(k) => add(out, v, Need::Keys(BTreeSet::from([k.clone()]))),
                        MapItem::Lit(_, x) => needs(x, pass, out),
                        MapItem::Var(x) => needs(&Expr::Var(x.clone()), pass, out),
                    }
                }
                // The base itself: its keys only.
                add(out, v, Need::Keys(BTreeSet::new()));
            } else {
                needs(b, false, out);
                for it in items {
                    match it {
                        MapItem::Lit(_, x) => needs(x, pass, out),
                        MapItem::Var(x) => needs(&Expr::Var(x.clone()), pass, out),
                        _ => {}
                    }
                }
            }
        }
        // Patterns bind their nodes by id; subqueries read what their own
        // clauses need.
        Expr::Lit(_)
        | Expr::Param(_)
        | Expr::CountStar
        | Expr::Pattern(_)
        | Expr::Slot(_)
        | Expr::Subquery(..) => {}
    }
}

/// The needs of a comprehension over `list` with local `var`: what the
/// body needs of `var` applies to the list's elements.
fn local(
    var: &str,
    list: &Expr,
    mut inner: HashMap<String, Need>,
    out: &mut HashMap<String, Need>,
) {
    let mine = inner.remove(var);
    for (k, n) in inner {
        add(out, &k, n);
    }
    match (mine, list) {
        (None, l) => needs(l, true, out),
        (Some(n), Expr::Var(v)) => add(out, v, n),
        (Some(_), l) => needs(l, false, out),
    }
}

/// The needs of several expressions, each evaluated for its value.
pub(crate) fn needs_of(exprs: &[&Expr]) -> HashMap<String, Need> {
    let mut out = HashMap::new();
    for e in exprs {
        needs(e, true, &mut out);
    }
    out
}

/// Late-read requests: nodes and relationships, each with the keys
/// still to read (`None`: every property).
#[derive(Default)]
struct Wanted {
    nodes: HashMap<u64, Option<BTreeSet<String>>>,
    rels: HashMap<u64, (u64, Option<BTreeSet<String>>)>,
}

fn want_keys(slot: &mut Option<BTreeSet<String>>, add: Option<BTreeSet<String>>, fresh: bool) {
    match (add, fresh) {
        (None, _) => *slot = None,
        (Some(k), true) => *slot = Some(k),
        (Some(k), false) => {
            if let Some(s) = slot {
                s.extend(k);
            }
        }
    }
}

/// The keys of `need` missing from an element with `partial` keys:
/// `None` when nothing is missing, `Some(None)` for all of them.
#[allow(clippy::option_option)]
fn missing(need: &Need, partial: Option<&Arc<[String]>>) -> Option<Option<BTreeSet<String>>> {
    let have = partial?;
    match need {
        Need::All => Some(None),
        Need::Keys(ks) => {
            let no_labels = have.iter().any(|h| h == NO_LABELS);
            let m: BTreeSet<String> = ks
                .iter()
                .filter(|k| {
                    if real(k) {
                        !have.iter().any(|h| h == *k)
                    } else {
                        *k == LABELS && no_labels
                    }
                })
                .cloned()
                .collect();
            (!m.is_empty()).then_some(Some(m))
        }
    }
}

impl Exec<'_> {
    /// The best-read copy of node `n` this statement has.
    fn best_node(&self, n: &Arc<Node>) -> Arc<Node> {
        if n.partial.is_none() {
            return Arc::clone(n);
        }
        match self.seen_nodes.get(&n.id) {
            Some(b) if !Arc::ptr_eq(b, n) && more(b.partial.as_ref(), n.partial.as_ref()) => {
                Arc::clone(b)
            }
            _ => Arc::clone(n),
        }
    }

    fn best_rel(&self, r: &Arc<Rel>) -> Arc<Rel> {
        if r.partial.is_none() {
            return Arc::clone(r);
        }
        match self.seen_rels.get(&r.id) {
            Some(b) if more(b.partial.as_ref(), r.partial.as_ref()) => Arc::clone(b),
            _ => Arc::clone(r),
        }
    }

    fn want(&self, v: &Value, need: &Need, w: &mut Wanted) {
        match v {
            Value::Node(n) => {
                let n = self.best_node(n);
                if let Some(m) = missing(need, n.partial.as_ref()) {
                    let fresh = !w.nodes.contains_key(&n.id);
                    let slot = w.nodes.entry(n.id).or_insert(Some(BTreeSet::new()));
                    want_keys(slot, m, fresh);
                }
            }
            Value::Rel(r) => {
                let r = self.best_rel(r);
                if let Some(m) = missing(need, r.partial.as_ref()) {
                    let fresh = !w.rels.contains_key(&r.id);
                    let slot = w
                        .rels
                        .entry(r.id)
                        .or_insert((r.src_rk, Some(BTreeSet::new())));
                    want_keys(&mut slot.1, m, fresh);
                }
            }
            Value::List(l) => {
                for x in l {
                    self.want(x, need, w);
                }
            }
            Value::Map(m) => {
                for x in m.values() {
                    self.want(x, &Need::All, w);
                }
            }
            Value::Path(p) => {
                for n in &p.nodes {
                    self.want(&Value::Node(Arc::clone(n)), &Need::All, w);
                }
                for r in &p.rels {
                    self.want(&Value::Rel(Arc::clone(r)), &Need::All, w);
                }
            }
            _ => {}
        }
    }

    /// Replaces elements in `v` by the best-read copies.
    fn upgrade(&self, v: &mut Value) {
        match v {
            Value::Node(n) => {
                let b = self.best_node(n);
                if !Arc::ptr_eq(&b, n) {
                    *n = b;
                }
            }
            Value::Rel(r) => {
                let b = self.best_rel(r);
                if !Arc::ptr_eq(&b, r) {
                    *r = b;
                }
            }
            Value::List(l) => {
                for x in l {
                    self.upgrade(x);
                }
            }
            Value::Map(m) => {
                for x in m.values_mut() {
                    self.upgrade(x);
                }
            }
            Value::Path(p) => {
                if p.nodes.iter().any(|n| n.partial.is_some())
                    || p.rels.iter().any(|r| r.partial.is_some())
                {
                    let p = Arc::make_mut(p);
                    for n in &mut p.nodes {
                        *n = self.best_node(n);
                    }
                    for r in &mut p.rels {
                        *r = self.best_rel(r);
                    }
                }
            }
            _ => {}
        }
    }

    /// Reads what `w` asks for, in batches, into the statement's copies.
    async fn fetch(&mut self, w: Wanted) -> Result<()> {
        // One read per key set: all properties, or the union of the keys
        // asked for (each node gets at least what it needs).
        let mut all: Vec<u64> = Vec::new();
        let mut keyed: Vec<u64> = Vec::new();
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for (id, k) in w.nodes {
            match k {
                None => all.push(id),
                Some(k) => {
                    keyed.push(id);
                    keys.extend(k);
                }
            }
        }
        let batch = self.id_batch();
        for (ids, ks) in [(all, None), (keyed, Some(keys))] {
            let ks: Option<Vec<String>> = ks.map(|s| s.into_iter().filter(|k| real(k)).collect());
            let mut ids = ids;
            ids.sort_unstable();
            for chunk in ids.chunks(batch) {
                let rows = self.store.nodes_part(chunk, ks.as_deref()).await?;
                for r in rows {
                    let n = self.merged_node(&r, ks.as_deref());
                    self.seen_nodes.insert(n.id, n);
                }
            }
        }
        let mut all: Vec<(u64, u64)> = Vec::new();
        let mut keyed: Vec<(u64, u64)> = Vec::new();
        let mut keys: BTreeSet<String> = BTreeSet::new();
        for (id, (rk, k)) in w.rels {
            match k {
                None => all.push((rk, id)),
                Some(k) => {
                    keyed.push((rk, id));
                    keys.extend(k);
                }
            }
        }
        for (at, ks) in [(all, None), (keyed, Some(keys))] {
            let ks: Option<Vec<String>> = ks.map(|s| s.into_iter().collect());
            let mut at = at;
            at.sort_unstable();
            for chunk in at.chunks(batch) {
                let rows = self.store.edges_part(chunk, ks.as_deref()).await?;
                for e in rows {
                    let r = self.merged_rel(&e, ks.as_deref());
                    self.seen_rels.insert(r.id, r);
                }
            }
        }
        Ok(())
    }

    /// A node read with `keys` (`None`: all), merged with what the
    /// statement already read of it.
    pub(crate) fn merged_node(&self, r: &NodeRow, keys: Option<&[String]>) -> Arc<Node> {
        let fresh = self.node_val(r);
        let Some(keys) = keys else { return fresh };
        let mut n = (*fresh).clone();
        let mut have: Vec<String> = keys.to_vec();
        if let Some(old) = self.seen_nodes.get(&r.id) {
            match &old.partial {
                None => return Arc::clone(old),
                Some(ok) => {
                    for k in ok.iter() {
                        // Labels come with every read.
                        if k != NO_LABELS && !have.contains(k) {
                            have.push(k.clone());
                            if let Some(v) = old.props.get(k) {
                                n.props.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
            }
        }
        n.partial = Some(have.into());
        Arc::new(n)
    }

    fn merged_rel(&self, e: &EdgeRow, keys: Option<&[String]>) -> Arc<Rel> {
        let fresh = self.rel_val(e);
        let Some(keys) = keys else { return fresh };
        let mut r = (*fresh).clone();
        let mut have: Vec<String> = keys.to_vec();
        if let Some(old) = self.seen_rels.get(&e.id) {
            match &old.partial {
                None => return Arc::clone(old),
                Some(ok) => {
                    for k in ok.iter() {
                        if !have.contains(k) {
                            have.push(k.clone());
                            if let Some(v) = old.props.get(k) {
                                r.props.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
            }
        }
        r.partial = Some(have.into());
        Arc::new(r)
    }

    /// Reads, for all `rows` at once, the properties `exprs` need (see
    /// the module docs).
    pub(crate) async fn late(&mut self, rows: &mut [Row], exprs: &[&Expr]) -> Result<()> {
        if !self.late || rows.is_empty() {
            return Ok(());
        }
        let need = needs_of(exprs);
        self.late_needs(rows, &need).await
    }

    /// [`Exec::late`] for needs already worked out.
    pub(crate) async fn late_needs(
        &mut self,
        rows: &mut [Row],
        need: &HashMap<String, Need>,
    ) -> Result<()> {
        if !self.late || rows.is_empty() || need.is_empty() {
            return Ok(());
        }
        let mut w = Wanted::default();
        for r in rows.iter() {
            for (v, n) in need {
                if let Some(x) = r.get(v) {
                    self.want(x, n, &mut w);
                }
            }
        }
        if !w.nodes.is_empty() || !w.rels.is_empty() {
            self.fetch(w).await?;
        }
        for r in rows.iter_mut() {
            for v in need.keys() {
                if let Some(x) = r.get_mut(v) {
                    self.upgrade(x);
                }
            }
        }
        Ok(())
    }

    /// Reads every property of the elements in the rows a statement
    /// returns.
    pub(crate) async fn late_rows(&mut self, rows: &mut [Vec<Value>]) -> Result<()> {
        if !self.late {
            return Ok(());
        }
        let mut w = Wanted::default();
        for r in rows.iter() {
            for x in r {
                self.want(x, &Need::All, &mut w);
            }
        }
        // Copies read since may stand for elements nothing asks to read
        // now: the rows get the best copies either way.
        if !w.nodes.is_empty() || !w.rels.is_empty() {
            self.fetch(w).await?;
        }
        for r in rows.iter_mut() {
            for x in r {
                self.upgrade(x);
            }
        }
        Ok(())
    }

    /// Ids per statement when reading elements by id.
    pub(crate) fn id_batch(&self) -> usize {
        self.b.batch_nodes.max(1).saturating_mul(5)
    }

    /// Nodes by id for binding to `var`, read late: with the keys the
    /// clause is known to read of `var` next ([`Exec::prefetch`]), else
    /// with none (ids, hashes and labels).
    pub(crate) async fn read_nodes(
        &mut self,
        ids: &[u64],
        var: Option<&String>,
        extra: Option<&Need>,
    ) -> Result<HashMap<u64, Arc<Node>>> {
        let need = self.need_of(var, extra);
        let only = var.and_then(|v| self.only.get(v)).cloned();
        let mut out: HashMap<u64, Arc<Node>> = HashMap::new();
        let mut want: Vec<u64> = Vec::new();
        for id in ids {
            if only.as_ref().is_some_and(|o| o.binary_search(id).is_err()) {
                continue;
            }
            match self.seen_nodes.get(id) {
                Some(n)
                    if need
                        .as_ref()
                        .is_none_or(|nd| missing(nd, n.partial.as_ref()).is_none()) =>
                {
                    out.insert(*id, Arc::clone(n));
                }
                _ => want.push(*id),
            }
        }
        want.sort_unstable();
        want.dedup();
        let keys: Option<Vec<String>> = match &need {
            None => Some(Vec::new()),
            Some(Need::All) => None,
            Some(Need::Keys(ks)) => Some(ks.iter().filter(|k| real(k)).cloned().collect()),
        };
        let filter = var
            .and_then(|v| self.push.get(v))
            .cloned()
            .unwrap_or_default();
        let batch = self.id_batch();
        for chunk in want.chunks(batch) {
            for r in self
                .store
                .nodes_where(chunk, keys.as_deref(), &filter)
                .await?
            {
                let n = match keys.as_deref() {
                    Some([]) => self.bare_node(&r),
                    k => {
                        let n = self.merged_node(&r, k);
                        self.seen_nodes.insert(n.id, Arc::clone(&n));
                        n
                    }
                };
                out.insert(r.id, n);
            }
        }
        Ok(out)
    }

    /// What reading nodes for `var` brings with them: the clause's
    /// prefetch for `var` and `extra`.
    pub(crate) fn need_of(&self, var: Option<&String>, extra: Option<&Need>) -> Option<Need> {
        let mut need = var.and_then(|v| self.prefetch.get(v)).cloned();
        if let Some(x) = extra {
            match &mut need {
                Some(n) => n.merge(x.clone()),
                None => need = Some(x.clone()),
            }
        }
        need
    }

    /// A far node read without properties: the best copy the statement
    /// has, else a late one with none.
    pub(crate) fn bare_node(&self, r: &NodeRow) -> Arc<Node> {
        if let Some(n) = self.seen_nodes.get(&r.id) {
            return Arc::clone(n);
        }
        let mut n = (*self.node_val(r)).clone();
        n.props.clear();
        n.partial = Some(Arc::from(Vec::<String>::new()));
        Arc::new(n)
    }
}

/// Whether `a` holds more properties than `b`.
fn more(a: Option<&Arc<[String]>>, b: Option<&Arc<[String]>>) -> bool {
    match (a, b) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(a), Some(b)) => {
            if Arc::ptr_eq(a, b) {
                return false;
            }
            let labels = |p: &Arc<[String]>| !p.iter().any(|k| k == NO_LABELS);
            let (al, bl) = (labels(a), labels(b));
            if bl && !al {
                return false;
            }
            let mut bk = 0usize;
            for k in b.iter().filter(|k| real(k)) {
                bk += 1;
                if !a.iter().any(|x| x == k) {
                    return false;
                }
            }
            let ak = a.iter().filter(|k| real(k)).count();
            ak > bk || (al && !bl)
        }
    }
}
