//! Record hierarchies across tenants (decisions T27 and T28; T22's piece
//! 7).
//!
//! A child tenant's root space inherits its parent tenant's root space, so
//! a structural change in a tenant's root space reaches every tenant below
//! it whose edges for the table are unbroken. Inside the change's
//! transaction, after the tenant's own spaces were reconciled:
//!
//! ```text
//! check(change in tenant p's root space):
//!   the tenants below p holding the table, with unbroken tenant edges,
//!     top first: lock each (its __sprig_hlock row, as the system, held
//!     until p's transaction ends)
//!   every level L below (a tenant x and a space of x that inherits
//!     through x's root), with the layers between p's root and L:
//!     CHANGED k (inserted or moved; its parent q in p's view now), unless
//!       a layer of L sets k's own parent or hides k:
//!       q hidden in L                                       → 39712
//!       walking up from q by L's parent links meets k       → 39711
//!       q's depth in L + 1 over MAX DEPTH                   → 39713
//!   every tenant x below, DELETED D:
//!     a row of x (own, orphan, or an override setting the parent) under a
//!     record of D, or an override of a record of D in x's root space that
//!     the copy worker will orphan, under a record of D      → 39714
//! ```
//!
//! L's parent link of a key is the nearest layer's (an own or orphan row,
//! or an override with the parent's bit; a hide drops the record), else
//! p's merged view's, read inside p's transaction. The layers are read as
//! the provisioning account, committed: they can't change while their
//! tenants' locks are held. Once p commits, the copy worker brings each
//! tenant's paths up to date ([`crate::copy::Maintainer::on_keys`]).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::model::HierarchyDef;
use sprig_types::rewrite::key_literal;
use sprig_types::{HierChange, Rows};

use crate::Runner;
use crate::grammar::unsupported;
use crate::model::{InheritState, InheritableObject, ROOT_SPACE, SpaceId};
use crate::plan::{Ctx, Resolved, state_of, tenant_edge};
use crate::view::{ViewSql, q};

/// Keys per statement.
const BATCH: usize = 1000;

/// A tenant below the session's that inherits the table.
struct Below {
    tenant: TenantId,
    name: String,
    schema: String,
    obj: Arc<InheritableObject>,
    /// The tenants between the session's and this one, nearest to the
    /// session's first (indices into the list).
    above: Vec<usize>,
    /// Its spaces that inherit through its root: the space's name and its
    /// chain, the space first, the root last.
    levels: Vec<(String, Vec<SpaceId>)>,
}

/// One layer row, as the checks need it.
#[derive(Debug, Clone)]
struct LayerRow {
    state: String,
    parent: Option<String>,
    ovr: u64,
}

/// A level's parent link of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Link {
    /// Not read yet.
    Unknown,
    /// Not in the level's view.
    Absent,
    /// In the view, under this parent (`None`: a top row).
    Parent(Option<String>),
}

/// The space chain of `sp` in `st` up to the root, when `sp`'s view of
/// `o` reaches the root (every space on the way receives it).
fn chain_to_root(st: &InheritState, o: &InheritableObject, sp: SpaceId) -> Option<Vec<SpaceId>> {
    let mut out = vec![sp];
    let mut cur = sp;
    while cur != ROOT_SPACE {
        st.edge(o.object_id, cur)?;
        cur = st.space(cur, "")?.parent.unwrap_or(ROOT_SPACE);
        if out.len() > 4096 {
            return None;
        }
        out.push(cur);
    }
    Some(out)
}

/// The tenants below the session's that inherit `res`'s table through
/// unbroken tenant edges, top first, with their catalogs.
async fn below_tenants(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
) -> Result<Vec<(Below, Arc<TenantCatalog>)>> {
    let me = ctx.cat.tenant;
    let table = &res.obj.table;
    let mut tree = r.tree_tables(me, &res.db_name, table).await?;
    tree.retain(|t| t.tenant != me && t.ancestors.contains(&me));
    tree.sort_by_key(|t| t.ancestors.len());
    let mut out: Vec<(Below, Arc<TenantCatalog>)> = Vec::new();
    for t in tree {
        let Some(pos) = t.ancestors.iter().position(|a| *a == me) else {
            continue;
        };
        let mut above = Vec::new();
        let mut unbroken = true;
        for b in &t.ancestors[pos + 1..] {
            let Some(i) = out.iter().position(|(x, _)| x.tenant == *b) else {
                unbroken = false;
                break;
            };
            above.push(i);
        }
        if !unbroken {
            continue;
        }
        let cat = r.tenant_catalog(t.tenant).await?;
        let edge = match above.last() {
            Some(&i) => tenant_edge(&out[i].1, &cat, &res.db_name, table, ctx.lctn)?,
            None => tenant_edge(ctx.cat, &cat, &res.db_name, table, ctx.lctn)?,
        };
        if edge.is_none() {
            continue;
        }
        let Some(st) = state_of(&cat) else {
            continue;
        };
        let Some(d) = cat.database(&sprig_catalog::ddl::database_key(&res.db_name, ctx.lctn))
        else {
            continue;
        };
        let Some(o) = st.object_named(d.db_id, table, ctx.lctn).cloned() else {
            continue;
        };
        let mut levels = Vec::new();
        let mut spaces = vec![ROOT_SPACE];
        spaces.extend(st.descendants(ROOT_SPACE));
        for sp in spaces {
            if let Some(c) = chain_to_root(st, &o, sp) {
                let name = st
                    .space(sp, &t.name)
                    .map_or_else(|| sp.to_string(), |s| s.name);
                levels.push((name, c));
            }
        }
        out.push((
            Below {
                tenant: t.tenant,
                name: t.name.clone(),
                schema: t.schema.clone(),
                obj: o,
                above,
                levels,
            },
            cat,
        ));
    }
    Ok(out)
}

fn text(v: Option<&Option<Vec<u8>>>) -> Option<String> {
    v.and_then(Option::as_ref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn refused(code: SprigCode, msg: String) -> SprigError {
    SprigError::new(code, msg)
}

/// What [`check`] reads, cached across its rounds.
struct Reader<'a> {
    ctx: &'a Ctx<'a>,
    res: &'a Resolved,
    h: &'a HierarchyDef,
    below: &'a [(Below, Arc<TenantCatalog>)],
    /// (tenant index, space, key) → its layer row.
    rows: HashMap<(usize, u64, String), LayerRow>,
    /// The session's root view: key → its link there.
    top: HashMap<String, Link>,
    /// Keys read everywhere.
    read: BTreeSet<String>,
}

impl Reader<'_> {
    fn lit(&self, raw: &[u8]) -> String {
        key_literal(raw, self.h.key_class, self.ctx.lex.no_backslash_escapes)
    }

    /// Reads `keys` in every tenant below and in the session's root view.
    async fn read(&mut self, r: &mut dyn Runner, keys: &BTreeSet<String>) -> Result<()> {
        let fresh: Vec<String> = keys.difference(&self.read).cloned().collect();
        if fresh.is_empty() {
            return Ok(());
        }
        let k = q(&self.h.key);
        let p = q(&self.h.parent);
        for chunk in fresh.chunks(BATCH) {
            // The session's root view, inside its transaction.
            let chain = self
                .ctx
                .chain(&self.res.obj, ROOT_SPACE, &self.res.prefix)?;
            let view = ViewSql {
                o: &self.res.obj,
                layer: self.res.layer.clone(),
                keys: Some(chunk.to_vec()),
                origin: false,
                derived: Vec::new(),
            }
            .view(&chain);
            let rows: Rows = r
                .query(&format!("SELECT {k}, {p} FROM ({view}) AS `__sprig_x`"))
                .await?;
            for key in chunk {
                self.top.insert(key.clone(), Link::Absent);
            }
            for row in &rows.rows {
                let key = self.lit(
                    row.first()
                        .cloned()
                        .flatten()
                        .unwrap_or_default()
                        .as_slice(),
                );
                let parent = row
                    .get(1)
                    .cloned()
                    .flatten()
                    .map(|v| self.lit(v.as_slice()));
                self.top.insert(key, Link::Parent(parent));
            }
            // Each tenant's layer rows, committed.
            for (i, (b, _)) in self.below.iter().enumerate() {
                let sql = format!(
                    "SELECT {k}, {p}, `_sprig_space`, CAST(`_sprig_state` AS CHAR), `_sprig_ovr` \
                     FROM {}.{} WHERE {k} IN ({})",
                    q(&b.schema),
                    q(&b.obj.table),
                    chunk.join(", ")
                );
                for row in r.ctl_query(b.tenant, &sql).await? {
                    let key = self.lit(
                        row.first()
                            .cloned()
                            .flatten()
                            .unwrap_or_default()
                            .as_slice(),
                    );
                    let parent = row
                        .get(1)
                        .cloned()
                        .flatten()
                        .map(|v| self.lit(v.as_slice()));
                    let space = text(row.get(2))
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0u64);
                    let state = text(row.get(3)).unwrap_or_default();
                    let ovr = text(row.get(4))
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0u64);
                    self.rows
                        .insert((i, space, key), LayerRow { state, parent, ovr });
                }
            }
        }
        self.read.extend(fresh);
        Ok(())
    }

    /// The layers of level `(x, level)`, nearest first: (tenant index,
    /// spaces nearest first).
    fn stack(&self, x: usize, level: usize) -> Vec<(usize, Vec<SpaceId>)> {
        let b = &self.below[x].0;
        let mut out = vec![(x, b.levels[level].1.clone())];
        for &i in b.above.iter().rev() {
            out.push((i, vec![ROOT_SPACE]));
        }
        out
    }

    /// Whether a layer of the stack sets `k`'s own parent or hides it.
    fn own_link(&self, stack: &[(usize, Vec<SpaceId>)], k: &str) -> Option<Link> {
        for (i, spaces) in stack {
            let bit = self.below[*i].0.obj.bit(&self.h.parent);
            for sp in spaces {
                if let Some(row) = self.rows.get(&(*i, sp.0, k.to_owned())) {
                    match row.state.as_str() {
                        "hide" => return Some(Link::Absent),
                        "own" | "orphan" => return Some(Link::Parent(row.parent.clone())),
                        "ovr" if row.ovr & bit != 0 => {
                            return Some(Link::Parent(row.parent.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
        None
    }

    fn link(&self, stack: &[(usize, Vec<SpaceId>)], k: &str) -> Link {
        if !self.read.contains(k) {
            return Link::Unknown;
        }
        if let Some(l) = self.own_link(stack, k) {
            return l;
        }
        self.top.get(k).cloned().unwrap_or(Link::Unknown)
    }
}

/// Where one changed key's walk in one level stands.
struct Walk {
    x: usize,
    level: usize,
    key: String,
    /// The key's new parent in the session's view, once known.
    parent: Option<String>,
    /// The record the walk reached, and its depth above `parent`.
    cur: Option<String>,
    depth: i64,
    started: bool,
}

/// Checks the tenants below the session's (see the module docs) for a
/// change in its root space, under their locks.
///
/// # Errors
/// 39711-39714; the backends'.
pub(crate) async fn check(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    h: &HierarchyDef,
    change: &HierChange,
) -> Result<()> {
    let keys = match change {
        HierChange::Changed(k) | HierChange::Deleted(k) if !k.is_empty() => k,
        _ => return Ok(()),
    };
    let below = below_tenants(ctx, r, res).await?;
    if below.is_empty() {
        return Ok(());
    }
    // Their structure can't change until this transaction ends.
    for (b, _) in &below {
        r.hier_lock(b.tenant, &b.schema, b.obj.object_id.0).await?;
    }
    metrics::counter!("sprig_inherit_hierarchy_tenant_checks_total").increment(below.len() as u64);
    if matches!(change, HierChange::Deleted(_)) {
        return deleted(ctx, r, h, &below, keys).await;
    }
    let mut rd = Reader {
        ctx,
        res,
        h,
        below: &below,
        rows: HashMap::new(),
        top: HashMap::new(),
        read: BTreeSet::new(),
    };
    let mut walks: Vec<Walk> = Vec::new();
    for (x, (b, _)) in below.iter().enumerate() {
        for level in 0..b.levels.len() {
            for k in keys {
                walks.push(Walk {
                    x,
                    level,
                    key: k.clone(),
                    parent: None,
                    cur: None,
                    depth: 0,
                    started: false,
                });
            }
        }
    }
    let mut need: BTreeSet<String> = keys.iter().cloned().collect();
    let max = i64::from(h.max_depth);
    for _ in 0..=(u32::from(h.max_depth) + 3) {
        rd.read(r, &need).await?;
        need.clear();
        let mut open = Vec::new();
        for mut w in walks {
            let stack = rd.stack(w.x, w.level);
            let (b, _) = &below[w.x];
            let level = &b.levels[w.level].0;
            let place = || format!("in space '{level}' of tenant '{}'", b.name);
            if !w.started {
                if !rd.read.contains(&w.key) {
                    need.insert(w.key.clone());
                    open.push(w);
                    continue;
                }
                // A level that sets the record's parent itself, or hides
                // it, keeps its own structure for it.
                if rd.own_link(&stack, &w.key).is_some() {
                    continue;
                }
                // A top row, or none in the session's view: nothing to
                // walk.
                let Some(Link::Parent(Some(q))) = rd.top.get(&w.key).cloned() else {
                    continue;
                };
                w.parent = Some(q.clone());
                w.cur = Some(q);
                w.started = true;
            }
            let Some(cur) = w.cur.clone() else {
                continue;
            };
            match rd.link(&stack, &cur) {
                Link::Unknown => {
                    need.insert(cur);
                    open.push(w);
                }
                Link::Absent => {
                    if w.depth == 0 {
                        return Err(refused(
                            SprigCode::HierarchyParentNotFound,
                            format!(
                                "Parent {cur} of record {} of '{}' isn't visible {}",
                                w.key,
                                h.table,
                                place()
                            ),
                        ));
                    }
                    // An ancestor this level doesn't see: its own tree
                    // was cut before; nothing to check above it.
                }
                Link::Parent(None) => {
                    if w.depth + 1 > max {
                        return Err(refused(
                            SprigCode::HierarchyTooDeep,
                            format!(
                                "Record {} of '{}' would be deeper than MAX DEPTH {} {}",
                                w.key,
                                h.table,
                                h.max_depth,
                                place()
                            ),
                        ));
                    }
                }
                Link::Parent(Some(n)) => {
                    if n == w.key {
                        return Err(refused(
                            SprigCode::HierarchyCycle,
                            format!(
                                "Record {} of '{}' would be under its own descendant {} {}",
                                w.key,
                                h.table,
                                w.parent.clone().unwrap_or_default(),
                                place()
                            ),
                        ));
                    }
                    w.depth += 1;
                    if w.depth > max + 1 {
                        return Err(refused(
                            SprigCode::HierarchyTooDeep,
                            format!(
                                "Record {} of '{}' would be deeper than MAX DEPTH {} {}",
                                w.key,
                                h.table,
                                h.max_depth,
                                place()
                            ),
                        ));
                    }
                    need.insert(n.clone());
                    w.cur = Some(n);
                    open.push(w);
                }
            }
        }
        walks = open;
        if walks.is_empty() {
            return Ok(());
        }
    }
    Err(refused(
        SprigCode::HierarchyTooDeep,
        format!(
            "'{}' would be deeper than MAX DEPTH {} in a tenant below",
            h.table, h.max_depth
        ),
    ))
}

/// The `DELETED` check of [`check`].
async fn deleted(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    h: &HierarchyDef,
    below: &[(Below, Arc<TenantCatalog>)],
    keys: &[String],
) -> Result<()> {
    let k = q(&h.key);
    let p = q(&h.parent);
    let nbe = ctx.lex.no_backslash_escapes;
    for (b, _) in below {
        let pbit = b.obj.bit(&h.parent);
        let rest = !super::path_bits(&b.obj);
        for chunk in keys.chunks(BATCH) {
            let d = chunk.join(", ");
            let sql = format!(
                "SELECT {k}, `_sprig_space` FROM {}.{} WHERE ({p} IN ({d}) AND {k} NOT IN ({d}) \
                 AND (`_sprig_state` IN ('own', 'orphan') OR (`_sprig_state` = 'ovr' AND \
                 `_sprig_ovr` & {pbit} <> 0))) OR (`_sprig_space` = {} AND {k} IN ({d}) AND \
                 `_sprig_state` = 'ovr' AND `_sprig_ovr` & {rest} <> 0 AND {p} IN ({d})) LIMIT 1",
                q(&b.schema),
                q(&b.obj.table),
                ROOT_SPACE.0
            );
            if let Some(row) = r.ctl_query(b.tenant, &sql).await?.into_iter().next() {
                let key = key_literal(
                    row.first()
                        .cloned()
                        .flatten()
                        .unwrap_or_default()
                        .as_slice(),
                    h.key_class,
                    nbe,
                );
                return Err(refused(
                    SprigCode::HierarchyHasChildren,
                    format!(
                        "Record {key} of '{}' in tenant '{}' would be left under a deleted \
                         record: delete it, move it or SPRIG RESET it there first",
                        h.table, b.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Before `DROP SHARE` of a share that carries a record hierarchy to child
/// tenants: refused while a tenant below has structure of its own (its
/// tree would lose the rows above its own); otherwise its path overrides
/// go, so none becomes an own row.
///
/// # Errors
/// 39891; the backends'.
pub(crate) async fn before_unshare(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    carried: &[crate::unshare::Carried],
) -> Result<()> {
    let me = ctx.cat.tenant;
    let mut seen = BTreeSet::new();
    for c in carried {
        let Some(o) = ctx.state.object(c.object).filter(|o| super::has_paths(o)) else {
            continue;
        };
        let mut tree = r.tree_tables(me, &c.db, &c.table).await?;
        tree.retain(|t| t.tenant != me && t.ancestors.contains(&me));
        for t in tree {
            if !seen.insert((t.tenant, c.table.to_lowercase())) {
                continue;
            }
            let cat = r.tenant_catalog(t.tenant).await?;
            let Some(xo) = state_of(&cat).and_then(|st| {
                cat.database(&sprig_catalog::ddl::database_key(&c.db, ctx.lctn))
                    .and_then(|d| st.object_named(d.db_id, &c.table, ctx.lctn))
                    .cloned()
            }) else {
                continue;
            };
            let rest = !super::path_bits(&xo);
            let table = format!("{}.{}", q(&t.schema), q(&xo.table));
            let own = r
                .ctl_query(
                    t.tenant,
                    &format!(
                        "SELECT 1 FROM {table} WHERE `_sprig_state` IN ('own', 'orphan', 'hide') \
                         OR (`_sprig_state` = 'ovr' AND `_sprig_ovr` & {rest} <> 0) LIMIT 1"
                    ),
                )
                .await?;
            if !own.is_empty() {
                return Err(unsupported(format!(
                    "DROP SHARE of '{}', which has a record hierarchy, while tenant '{}' has rows \
                     or overrides of its own: delete or SPRIG RESET them there first",
                    o.table, t.name
                )));
            }
            r.ctl_query(
                t.tenant,
                &format!("DELETE FROM {table} WHERE `_sprig_state` = 'ovr'"),
            )
            .await?;
        }
    }
    Ok(())
}

/// Whether `e` is one of a hierarchy's refusals (a conflict the copy
/// worker can't refuse to anyone).
#[must_use]
pub fn is_conflict(e: &SprigError) -> bool {
    matches!(
        e.code(),
        Some(
            SprigCode::HierarchyCycle
                | SprigCode::HierarchyParentNotFound
                | SprigCode::HierarchyTooDeep
                | SprigCode::HierarchyHasChildren
                | SprigCode::SubtreeTooLarge
        )
    )
}

/// The runners' statements on one provisioning connection, physical names
/// (the copy worker's [`super::reconcile_tenant`]).
pub struct ConnRunner {
    conn: mysql_async::Conn,
}

impl ConnRunner {
    /// A runner on `conn`.
    #[must_use]
    pub fn new(conn: mysql_async::Conn) -> Self {
        Self { conn }
    }
}

#[allow(clippy::needless_pass_by_value)] // `map_err(conn_err)`
fn conn_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            let mut x = SprigError::backend(s.code, state, format!("{} ({})", s.message, s.code));
            x.retryable = matches!(s.code, 1205 | 1213);
            x
        }
        other => {
            let mut x = SprigError::new(SprigCode::BackendUnavailable, other.to_string());
            x.retryable = true;
            x
        }
    }
}

#[async_trait::async_trait]
impl sprig_types::Runner for ConnRunner {
    async fn query(&mut self, sql: &str) -> Result<Rows> {
        use mysql_async::prelude::Queryable;
        let rows: Vec<mysql_async::Row> = self.conn.query(sql).await.map_err(conn_err)?;
        Ok(Rows {
            columns: Vec::new(),
            rows: rows
                .into_iter()
                .map(|r| {
                    r.unwrap()
                        .into_iter()
                        .map(|v| match v {
                            mysql_async::Value::NULL => None,
                            mysql_async::Value::Bytes(b) => Some(b),
                            other => {
                                Some(other.as_sql(true).trim_matches('\'').as_bytes().to_vec())
                            }
                        })
                        .collect()
                })
                .collect(),
        })
    }

    async fn exec(&mut self, sql: &str) -> Result<sprig_types::Done> {
        use mysql_async::prelude::Queryable;
        self.conn.query_drop(sql).await.map_err(conn_err)?;
        Ok(sprig_types::Done {
            affected_rows: self.conn.affected_rows(),
            ..sprig_types::Done::default()
        })
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn found_rows(&self) -> bool {
        false
    }

    async fn object_id(
        &mut self,
        _db: Option<&str>,
        _table: &str,
    ) -> Result<Option<(sprig_common::ids::DbId, sprig_common::ids::ObjectId)>> {
        Err(SprigError::internal("no catalog in this runner"))
    }

    async fn commit_types(&mut self, _change: &sprig_types::TypesChange) -> Result<()> {
        Err(SprigError::internal("no catalog in this runner"))
    }
}

#[async_trait::async_trait]
impl Runner for ConnRunner {
    async fn commit(&mut self, _change: &crate::InheritChange) -> Result<()> {
        Err(SprigError::internal("no catalog in this runner"))
    }

    async fn ctl(&mut self, sql: &str) -> Result<()> {
        sprig_types::Runner::exec(self, sql).await.map(|_| ())
    }

    async fn child_tenant(&mut self, _name: &str) -> Result<Option<TenantId>> {
        Ok(None)
    }

    fn user_id(&self, _name: &str) -> Option<u32> {
        None
    }
}
