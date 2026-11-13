//! Record hierarchies on inheritable tables (decision T22): chapter 32's
//! stored paths across chapter 42's spaces.
//!
//! A row's path (`_sprig_hpath`, with `_sprig_hdepth` and `_sprig_rk`) is
//! computed from its ancestors, and each space sees its own ancestors: a
//! space may override a row's parent, add rows, or hide them. So each
//! space keeps its own paths, stored the way chapter 42 stores any field:
//!
//! - the three path columns are fields of the object, with slots;
//! - a space stores a **path override** (an `ovr` row with the three
//!   columns' bits) only where its path differs from the one it would
//!   inherit, and its own rows carry their paths as any own row does;
//! - a space that only consumes the tree stores nothing and is never
//!   written when its parent moves things.
//!
//! Chapter 32's plans run in the session's space as before (their
//! statements go through the merged view of that space). They tell this
//! module of each structural change with `ALTER TABLE t SPRIG RECONCILE
//! HIERARCHY ...` ([`sprig_types::HierChange`]), inside the plan's
//! transaction:
//!
//! ```text
//! LOCK               before the plan's first statement: one structural
//!                    change of the object at a time (a row of
//!                    __sprig_hlock, FOR UPDATE)
//! CHANGED (k, ...)   rows inserted or moved in space s: reconcile(x, keys)
//!                    for every space x below s that inherits through s,
//!                    top first
//! DELETED (k, ...)   rows deleted in s: the same; a row of x left under a
//!                    deleted record fails the statement (39714)
//! ADDED              ADD HIERARCHY filled s: every space below gets its
//!                    paths, walking from its top rows (39716 otherwise)
//!
//! reconcile(x, roots):
//!  1. read the roots in x's merged view; a root x doesn't see may have no
//!     child in x (39714)
//!  2. walk down x's view by parent links (never by stored paths, which may
//!     be stale), level by level; meeting a top again is a cycle (39711)
//!  3. each top's expected path is its parent's merged path in x plus its
//!     segment (parent hidden in x: 39712); each row below, its parent's
//!     expected path plus its segment (deeper than MAX DEPTH, or a path
//!     over 2048 bytes: 39713); rows whose path is still NULL (a fill under
//!     way) are skipped with their subtrees
//!  4. where x's merged path differs from the expected one: when x
//!     inherits exactly the expected values and holds a path override, the
//!     override's bits are cleared (x follows its parent again); otherwise
//!     a path override is upserted
//! ```
//!
//! Inside one tenant only: shares of a hierarchical object to child
//! tenants, from a parent tenant, through materialized copies, or with
//! filters or column lists are refused (39891), as T22's piece 6 says.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::hier::{HDEPTH, HPATH, MAX_PATH_BYTES, RK};
use sprig_types::model::HierarchyDef;
use sprig_types::rewrite::{hex_lit, segment};
use sprig_types::{Done, HierChange, Rows};

use crate::Runner;
use crate::grammar::{Name, unsupported};
use crate::model::{InheritableObject, SpaceId};
use crate::plan::{Ctx, Cut, Resolved, physical_schema};
use crate::view::{Top, ViewSql, q};

/// The path columns, as chapter 32 names them.
pub const PATH_COLUMNS: [&str; 3] = [HPATH, HDEPTH, RK];

/// The table holding one lock row per hierarchical inheritable object, in
/// the object's physical schema.
pub const LOCK_TABLE: &str = "__sprig_hlock";

/// Rows per written batch, and keys per walked frontier.
const BATCH: usize = 1000;

/// Whether `c` is one of the path columns.
#[must_use]
pub fn is_path_column(c: &str) -> bool {
    PATH_COLUMNS.iter().any(|p| p.eq_ignore_ascii_case(c))
}

/// Whether `o` carries a record hierarchy's paths as fields.
#[must_use]
pub fn has_paths(o: &InheritableObject) -> bool {
    o.column(HPATH).is_some()
}

/// The `_sprig_ovr` bits of the path columns.
#[must_use]
pub fn path_bits(o: &InheritableObject) -> u64 {
    PATH_COLUMNS.iter().fold(0, |b, c| b | o.bit(c))
}

/// `o` without its path columns: what a statement that doesn't name them
/// reads (a merged view is a derived table, where MariaDB's `INVISIBLE`
/// doesn't hide them from `*`).
#[must_use]
pub fn without_paths(o: &InheritableObject) -> InheritableObject {
    let mut x = o.clone();
    x.columns.retain(|c| !is_path_column(c));
    x
}

/// Whether every reserved name `sql` uses is a path column (a type plan's
/// statement on a record hierarchy).
#[must_use]
pub fn names_only_paths(sql: &str, lex: &sprig_sql::lex::LexOptions) -> bool {
    crate::grammar::identifiers(sql.as_bytes(), lex)
        .iter()
        .filter(|i| i.to_ascii_lowercase().starts_with("_sprig_") || i.starts_with("__sprig_"))
        .all(|i| is_path_column(i))
}

/// The hierarchy of `o`, by this tenant's types.
#[must_use]
pub fn hierarchy_of(ctx: &Ctx<'_>, o: &InheritableObject) -> Option<Arc<HierarchyDef>> {
    ctx.types()?.db(o.db)?.hierarchy(&o.table, ctx.lctn).cloned()
}

/// Whether the table `layer` names has the path columns (they're
/// `INVISIBLE` and `_sprig_`, so `SHOW COLUMNS` never lists them).
pub(crate) async fn probe_paths(r: &mut dyn Runner, layer: &str) -> bool {
    r.query(&format!(
        "SELECT `{HPATH}`, `{HDEPTH}`, `{RK}` FROM {layer} WHERE FALSE"
    ))
    .await
    .is_ok()
}

fn err_in(ctx: &Ctx<'_>, x: SpaceId, code: SprigCode, what: &str) -> SprigError {
    SprigError::new(code, format!("{what} in space '{}'", ctx.space_name(x)))
}

/// One row of a space's merged view.
#[derive(Debug, Clone)]
struct Node {
    /// The key, as a literal.
    key: String,
    /// The key's text value (for its segment).
    raw: Vec<u8>,
    /// The parent, as a literal.
    parent: Option<String>,
    path: Option<Vec<u8>>,
    depth: Option<i64>,
    rk: Option<String>,
}

fn nodes(rows: &Rows, nbe: bool) -> Vec<Node> {
    rows.rows
        .iter()
        .map(|row| {
            let raw = row.first().cloned().flatten().unwrap_or_default();
            let opt = |i: usize| {
                row.get(i)
                    .and_then(Option::as_ref)
                    .map(|_| crate::write::literal_of(rows, row, i, nbe))
            };
            Node {
                key: crate::write::literal_of(rows, row, 0, nbe),
                raw,
                parent: opt(1),
                path: row.get(2).cloned().flatten(),
                depth: row
                    .get(3)
                    .and_then(Option::as_ref)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok()),
                rk: opt(4),
            }
        })
        .collect()
}

/// What a space should hold for one row.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Want {
    path: Vec<u8>,
    depth: i64,
    rk: String,
}

/// A space's view of the object, for the statements below.
struct SpaceView<'a> {
    res: &'a Resolved,
    h: &'a HierarchyDef,
    x: SpaceId,
    chain: crate::view::Chain,
    nbe: bool,
}

impl SpaceView<'_> {
    fn view(&self, keys: Option<Vec<String>>) -> String {
        ViewSql {
            o: &self.res.obj,
            layer: self.res.layer.clone(),
            keys,
            origin: false,
            derived: Vec::new(),
        }
        .view(&self.chain)
    }

    fn select(&self) -> String {
        format!(
            "SELECT {}, {}, `{HPATH}`, `{HDEPTH}`, `{RK}`",
            q(&self.h.key),
            q(&self.h.parent)
        )
    }

    async fn rows_of(&self, r: &mut dyn Runner, keys: &[String]) -> Result<Vec<Node>> {
        let mut out = Vec::new();
        for chunk in keys.chunks(BATCH) {
            let rows = r
                .query(&format!(
                    "{} FROM ({}) AS `__sprig_x`",
                    self.select(),
                    self.view(Some(chunk.to_vec()))
                ))
                .await?;
            out.extend(nodes(&rows, self.nbe));
        }
        Ok(out)
    }

    async fn children_of(&self, r: &mut dyn Runner, parents: &[String]) -> Result<Vec<Node>> {
        let mut out = Vec::new();
        for chunk in parents.chunks(BATCH) {
            let rows = r
                .query(&format!(
                    "{} FROM ({}) AS `__sprig_x` WHERE {} IN ({})",
                    self.select(),
                    self.view(None),
                    q(&self.h.parent),
                    chunk.join(", ")
                ))
                .await?;
            out.extend(nodes(&rows, self.nbe));
        }
        Ok(out)
    }
}

/// The spaces below `s` whose view of `o` passes through `s`, top first.
fn below(ctx: &Ctx<'_>, o: &InheritableObject, s: SpaceId) -> Result<Vec<SpaceId>> {
    let mut out = Vec::new();
    for x in ctx.state.descendants(s) {
        let c = ctx.chain_with(o, x, "", Cut::Never)?;
        if c.levels.iter().any(|l| l.space == s) {
            out.push(x);
        }
    }
    Ok(out)
}

/// `ALTER TABLE t SPRIG RECONCILE HIERARCHY ...` (a type plan's).
///
/// # Errors
/// 39711-39714, 39716, 39891.
pub async fn follow(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    table: &Name,
    change: &HierChange,
) -> Result<Done> {
    if !ctx.from_types {
        return Err(unsupported(
            "SPRIG RECONCILE HIERARCHY is SprigDB's own statement",
        ));
    }
    let Some(res) = ctx.resolve(table)? else {
        return Ok(Done::default());
    };
    if matches!(change, HierChange::Lock) {
        lock(ctx, r, &res).await?;
        return Ok(Done::default());
    }
    let Some(h) = hierarchy_of(ctx, &res.obj) else {
        return Ok(Done::default());
    };
    let s = ctx.space;
    let mut written = 0u64;
    match change {
        HierChange::Lock => {}
        HierChange::Changed(keys) | HierChange::Deleted(keys) => {
            for x in below(ctx, &res.obj, s)? {
                written += reconcile(ctx, r, &res, &h, x, keys, false).await?.1;
            }
        }
        HierChange::Added => {
            for x in below(ctx, &res.obj, s)? {
                written += full(ctx, r, &res, &h, x).await?;
            }
        }
    }
    metrics::counter!("sprig_inherit_hierarchy_reconciles_total").increment(1);
    Ok(Done {
        affected_rows: written,
        ..Done::default()
    })
}

/// Serializes structural changes of the object: its row of
/// [`LOCK_TABLE`], `FOR UPDATE`, made on first use.
pub(crate) async fn lock(ctx: &Ctx<'_>, r: &mut dyn Runner, res: &Resolved) -> Result<()> {
    let sql = format!(
        "SELECT `object_id` FROM {}{} WHERE `object_id` = {} FOR UPDATE",
        res.prefix,
        q(LOCK_TABLE),
        res.obj.object_id.0
    );
    if let Ok(rows) = r.query(&sql).await
        && !rows.rows.is_empty()
    {
        return Ok(());
    }
    let schema = physical_schema(ctx.cat, &res.db_name, ctx.lctn)?;
    r.ctl(&format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (`object_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY) \
         ENGINE=InnoDB",
        q(&schema),
        q(LOCK_TABLE)
    ))
    .await?;
    r.ctl(&format!(
        "INSERT IGNORE INTO {}.{} (`object_id`) VALUES ({})",
        q(&schema),
        q(LOCK_TABLE),
        res.obj.object_id.0
    ))
    .await?;
    match r.query(&sql).await {
        Ok(rows) if !rows.rows.is_empty() => Ok(()),
        Ok(_) => Err(SprigError::internal("the hierarchy's lock row is missing")),
        // A user whose grants name single tables can't read the lock
        // table: the plan runs unserialized, as on a plain table.
        Err(e) if e.protocol().mysql_errno == 1142 => {
            tracing::warn!(object = res.obj.object_id.0, "hierarchy lock not readable: {}", e.message);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Every row of space `x`, from its top rows (`ADD HIERARCHY`): 39716
/// when some aren't reached.
async fn full(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    h: &HierarchyDef,
    x: SpaceId,
) -> Result<u64> {
    let nbe = ctx.lex.no_backslash_escapes;
    let chain = ctx.chain(&res.obj, x, &res.prefix)?;
    let sv = SpaceView {
        res,
        h,
        x,
        chain,
        nbe,
    };
    let all = r
        .query(&format!(
            "{} FROM ({}) AS `__sprig_x`",
            sv.select(),
            sv.view(None)
        ))
        .await?;
    let all = nodes(&all, nbe);
    let tops: Vec<String> = all
        .iter()
        .filter(|n| n.parent.is_none())
        .map(|n| n.key.clone())
        .collect();
    let (walked, n) = reconcile(ctx, r, res, h, x, &tops, true).await?;
    let left: Vec<String> = all
        .iter()
        .filter(|n| !walked.contains(&n.key))
        .take(100)
        .map(|n| n.key.clone())
        .collect();
    if !left.is_empty() {
        return Err(err_in(
            ctx,
            x,
            SprigCode::HierarchyBackfillFailed,
            &format!(
                "ADD HIERARCHY on '{}' failed: these records have a missing parent or a cycle: {}",
                h.table,
                left.join(", ")
            ),
        ));
    }
    Ok(n)
}

/// Brings space `x`'s paths of the subtrees under `roots` (literals) up
/// to date (see the module docs). `full`: rows whose path is `NULL` are
/// given one rather than skipped. Returns the keys walked and the rows
/// written.
///
/// # Errors
/// 39711-39714, 39891.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    h: &HierarchyDef,
    x: SpaceId,
    roots: &[String],
    full: bool,
) -> Result<(BTreeSet<String>, u64)> {
    let o = &res.obj;
    let nbe = ctx.lex.no_backslash_escapes;
    let chain = ctx.chain(o, x, &res.prefix)?;
    if matches!(chain.top, Top::Upstream { .. }) {
        return Err(unsupported(
            "a record hierarchy read through a copy or from a parent tenant is not supported yet",
        ));
    }
    let sv = SpaceView {
        res,
        h,
        x,
        chain,
        nbe,
    };
    let mut walked = BTreeSet::new();
    if roots.is_empty() {
        return Ok((walked, 0));
    }
    // 1. The roots x sees; the others may have no child here.
    let seen = sv.rows_of(r, roots).await?;
    let mut all: BTreeMap<String, Node> = seen.into_iter().map(|n| (n.key.clone(), n)).collect();
    let unseen: Vec<String> = roots
        .iter()
        .filter(|k| !all.contains_key(*k))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !unseen.is_empty()
        && let Some(c) = sv.children_of(r, &unseen).await?.into_iter().next()
    {
        return Err(err_in(
            ctx,
            x,
            SprigCode::HierarchyHasChildren,
            &format!(
                "Record {} of '{}' would be left under record {}, which is deleted or hidden",
                c.key,
                h.table,
                c.parent.unwrap_or_default()
            ),
        ));
    }
    let tops: BTreeSet<String> = all
        .values()
        .filter(|n| n.parent.as_ref().is_none_or(|p| !all.contains_key(p)))
        .map(|n| n.key.clone())
        .collect();
    // 2. Down x's view by parent links.
    let mut kids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut frontier: Vec<String> = all.keys().cloned().collect();
    let mut reached: BTreeSet<String> = tops.clone();
    let mut rounds = 0u32;
    while !frontier.is_empty() {
        rounds += 1;
        if rounds > u32::from(h.max_depth) + 2 {
            return Err(err_in(
                ctx,
                x,
                SprigCode::HierarchyTooDeep,
                &format!("'{}' would be deeper than MAX DEPTH {}", h.table, h.max_depth),
            ));
        }
        let found = sv.children_of(r, &frontier).await?;
        frontier.clear();
        for c in found {
            let parent = c.parent.clone().unwrap_or_default();
            if tops.contains(&c.key) {
                return Err(err_in(
                    ctx,
                    x,
                    SprigCode::HierarchyCycle,
                    &format!(
                        "Record {} of '{}' would be under its own descendant {parent}",
                        c.key, h.table
                    ),
                ));
            }
            kids.entry(parent).or_default().push(c.key.clone());
            reached.insert(c.key.clone());
            if !all.contains_key(&c.key) {
                frontier.push(c.key.clone());
                all.insert(c.key.clone(), c);
            }
        }
    }
    if let Some(k) = all.keys().find(|k| !reached.contains(*k)) {
        // Roots whose parents are each other's: a cycle no top leads to.
        return Err(err_in(
            ctx,
            x,
            SprigCode::HierarchyCycle,
            &format!("Record {k} of '{}' would be in a cycle", h.table),
        ));
    }
    // 3. The expected paths, top-down.
    let mut want: BTreeMap<String, Want> = BTreeMap::new();
    let mut queue: Vec<String> = Vec::new();
    for t in &tops {
        let n = &all[t];
        if !full && n.path.is_none() {
            continue;
        }
        let seg = segment(&n.raw, h.key_class);
        let w = match &n.parent {
            None => Want {
                path: format!("/{seg}/").into_bytes(),
                depth: 0,
                rk: n.key.clone(),
            },
            Some(p) => {
                let Some(pn) = sv.rows_of(r, std::slice::from_ref(p)).await?.into_iter().next()
                else {
                    return Err(err_in(
                        ctx,
                        x,
                        SprigCode::HierarchyParentNotFound,
                        &format!("Parent {p} of record {} of '{}' isn't visible", n.key, h.table),
                    ));
                };
                let (Some(pp), Some(pd), Some(prk)) = (pn.path, pn.depth, pn.rk) else {
                    continue;
                };
                let mut path = pp;
                path.extend_from_slice(seg.as_bytes());
                path.push(b'/');
                Want {
                    path,
                    depth: pd + 1,
                    rk: prk,
                }
            }
        };
        want.insert(t.clone(), w);
        queue.push(t.clone());
    }
    let mut i = 0;
    while i < queue.len() {
        let k = queue[i].clone();
        i += 1;
        walked.insert(k.clone());
        let w = want[&k].clone();
        if w.depth > i64::from(h.max_depth) || w.path.len() > MAX_PATH_BYTES {
            return Err(err_in(
                ctx,
                x,
                SprigCode::HierarchyTooDeep,
                &format!(
                    "Record {k} of '{}' would be deeper than MAX DEPTH {} or its path longer \
                     than {MAX_PATH_BYTES} bytes",
                    h.table, h.max_depth
                ),
            ));
        }
        for c in kids.get(&k).cloned().unwrap_or_default() {
            let n = &all[&c];
            if !full && n.path.is_none() {
                continue;
            }
            let mut path = w.path.clone();
            path.extend_from_slice(segment(&n.raw, h.key_class).as_bytes());
            path.push(b'/');
            want.insert(
                c.clone(),
                Want {
                    path,
                    depth: w.depth + 1,
                    rk: w.rk.clone(),
                },
            );
            queue.push(c);
        }
    }
    // 4. What differs.
    let differ: Vec<String> = want
        .iter()
        .filter(|(k, w)| {
            let n = &all[*k];
            n.path.as_deref() != Some(w.path.as_slice())
                || n.depth != Some(w.depth)
                || n.rk.as_deref() != Some(w.rk.as_str())
        })
        .map(|(k, _)| k.clone())
        .collect();
    if differ.is_empty() {
        return Ok((walked, 0));
    }
    let written = write(ctx, r, &sv, &want, &differ).await?;
    Ok((walked, written))
}

/// Step 4: clears or upserts path overrides of `differ` in the space.
async fn write(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    sv: &SpaceView<'_>,
    want: &BTreeMap<String, Want>,
    differ: &[String],
) -> Result<u64> {
    let o = &sv.res.obj;
    let h = sv.h;
    let x = sv.x;
    let k = q(&h.key);
    let pb = path_bits(o);
    // The space's own layer rows of these keys, and what it would inherit.
    let mut state: BTreeMap<String, (String, u64)> = BTreeMap::new();
    let mut inherited: BTreeMap<String, Want> = BTreeMap::new();
    for chunk in differ.chunks(BATCH) {
        let rows = r
            .query(&format!(
                "SELECT {k}, `_sprig_state`, `_sprig_ovr` FROM {} WHERE `_sprig_space` = {} AND \
                 {k} IN ({})",
                sv.res.layer,
                x.0,
                chunk.join(", ")
            ))
            .await?;
        for row in &rows.rows {
            let key = crate::write::literal_of(&rows, row, 0, sv.nbe);
            let txt = |i: usize| {
                row.get(i)
                    .and_then(Option::as_ref)
                    .map(|v| String::from_utf8_lossy(v).into_owned())
                    .unwrap_or_default()
            };
            state.insert(key, (txt(1), txt(2).parse().unwrap_or(0)));
        }
        let inh = ViewSql {
            o,
            layer: sv.res.layer.clone(),
            keys: Some(chunk.to_vec()),
            origin: false,
            derived: Vec::new(),
        }
        .inherited(&sv.chain);
        if let Some(inh) = inh {
            let rows = r
                .query(&format!(
                    "SELECT {k}, `{HPATH}`, `{HDEPTH}`, `{RK}` FROM ({inh}) AS `__sprig_i`"
                ))
                .await?;
            for row in &rows.rows {
                let key = crate::write::literal_of(&rows, row, 0, sv.nbe);
                let (Some(path), Some(depth)) = (
                    row.get(1).cloned().flatten(),
                    row.get(2)
                        .and_then(Option::as_ref)
                        .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok()),
                ) else {
                    continue;
                };
                if row.get(3).is_none_or(Option::is_none) {
                    continue;
                }
                let rk = crate::write::literal_of(&rows, row, 3, sv.nbe);
                inherited.insert(key, Want { path, depth, rk });
            }
        }
    }
    let mut clear = Vec::new();
    let mut set = Vec::new();
    for key in differ {
        let ovr = state.get(key).is_some_and(|(s, _)| s == "ovr");
        if ovr && inherited.get(key) == want.get(key) {
            clear.push(key.clone());
        } else {
            set.push(key.clone());
        }
    }
    let mut n = 0u64;
    for chunk in clear.chunks(BATCH) {
        let cond = format!(
            "`_sprig_space` = {} AND `_sprig_state` = 'ovr' AND {k} IN ({})",
            x.0,
            chunk.join(", ")
        );
        n += r
            .exec(&format!(
                "UPDATE {} SET `_sprig_ovr` = `_sprig_ovr` & {} WHERE {cond}",
                sv.res.layer, !pb
            ))
            .await?
            .affected_rows;
        r.exec(&format!(
            "DELETE FROM {} WHERE {cond} AND `_sprig_ovr` = 0",
            sv.res.layer
        ))
        .await?;
        metrics::counter!("sprig_inherit_hierarchy_overrides_total", "op" => "clear")
            .increment(chunk.len() as u64);
    }
    for chunk in set.chunks(BATCH) {
        let case = |f: &dyn Fn(&Want) -> String| {
            let arms: Vec<String> = chunk
                .iter()
                .map(|key| format!("WHEN {key} THEN {}", f(&want[key])))
                .collect();
            format!("CASE `__sprig_r`.{k} {} END", arms.join(" "))
        };
        let list: Vec<String> = o
            .columns
            .iter()
            .map(|c| {
                if c.eq_ignore_ascii_case(HPATH) {
                    case(&|w| hex_lit(&w.path))
                } else if c.eq_ignore_ascii_case(HDEPTH) {
                    case(&|w| w.depth.to_string())
                } else if c.eq_ignore_ascii_case(RK) {
                    case(&|w| w.rk.clone())
                } else {
                    format!("`__sprig_r`.{}", q(c))
                }
            })
            .collect();
        let cols: Vec<String> = o.columns.iter().map(|c| q(c)).collect();
        n += r
            .exec(&format!(
                "INSERT INTO {} ({}, `_sprig_space`, `_sprig_state`, `_sprig_ovr`) SELECT {}, {}, \
                 'ovr', {pb} FROM ({}) AS `__sprig_r` ON DUPLICATE KEY UPDATE `{HPATH}` = \
                 VALUES(`{HPATH}`), `{HDEPTH}` = VALUES(`{HDEPTH}`), `{RK}` = VALUES(`{RK}`), \
                 `_sprig_ovr` = `_sprig_ovr` | {pb}",
                sv.res.layer,
                cols.join(", "),
                list.join(", "),
                x.0,
                sv.view(Some(chunk.to_vec()))
            ))
            .await?
            .affected_rows;
        metrics::counter!("sprig_inherit_hierarchy_overrides_total", "op" => "set")
            .increment(chunk.len() as u64);
    }
    let _ = ctx;
    Ok(n)
}

/// Before `SPRIG RESET` of a hierarchical object in the session's space
/// (inside the reset's transaction): the hierarchy's lock, and the keys
/// whose layer rows the reset may change (`cond`: the reset's ` AND (...)`).
///
/// # Errors
/// The lock's and the read's.
pub(crate) async fn reset_keys(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    h: &HierarchyDef,
    cond: &str,
) -> Result<Vec<String>> {
    lock(ctx, r, res).await?;
    let rows = r
        .query(&format!(
            "SELECT {} FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('ovr', 'hide', \
             'orphan'){cond}",
            q(&h.key),
            res.layer,
            ctx.space.0
        ))
        .await?;
    Ok(rows
        .rows
        .iter()
        .map(|row| crate::write::literal_of(&rows, row, 0, ctx.lex.no_backslash_escapes))
        .collect())
}

/// After `SPRIG RESET`: the space and every space below it reconcile the
/// reset records (a reset `parent_id` changes the space's own tree).
///
/// # Errors
/// [`reconcile`]'s.
pub(crate) async fn after_reset(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    h: &HierarchyDef,
    keys: &[String],
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let s = ctx.space;
    reconcile(ctx, r, res, h, s, keys, false).await?;
    for x in below(ctx, &res.obj, s)? {
        reconcile(ctx, r, res, h, x, keys, false).await?;
    }
    Ok(())
}

/// `ADD HIERARCHY` on an inheritable table (its `ALTER TABLE` adding the
/// path columns): only from the space that owns every row, inside one
/// tenant (decision T22, piece 6).
///
/// # Errors
/// 39891.
pub(crate) async fn before_add(ctx: &Ctx<'_>, r: &mut dyn Runner, res: &Resolved) -> Result<()> {
    let o = &res.obj;
    refuse_tenants(ctx, o)?;
    for sh in ctx.state.shares_of(o.object_id) {
        if sh.filter.is_some() || sh.columns.is_some() || sh.take.is_some() {
            return Err(unsupported(format!(
                "ADD HIERARCHY on a table whose share '{}' has a filter, a column list or a TAKE",
                sh.name
            )));
        }
        if sh.mode == crate::model::MergeMode::Materialized {
            return Err(unsupported(format!(
                "ADD HIERARCHY on a table whose share '{}' is materialized",
                sh.name
            )));
        }
    }
    let chain = ctx.chain(o, ctx.space, &res.prefix)?;
    if !chain.is_base() {
        return Err(unsupported(
            "ADD HIERARCHY on an inheritable table runs in the space that owns its rows",
        ));
    }
    let other = r
        .query(&format!(
            "SELECT 1 FROM {} WHERE `_sprig_space` <> {} AND `_sprig_state` IN ('own', 'orphan') \
             LIMIT 1",
            res.layer, ctx.space.0
        ))
        .await?;
    if !other.rows.is_empty() {
        return Err(unsupported(
            "ADD HIERARCHY on an inheritable table while other spaces have their own rows",
        ));
    }
    Ok(())
}

/// `DROP HIERARCHY` on an inheritable table: path overrides go (every
/// space's), and the path bits of other overrides are cleared, so no
/// space keeps a record only for its path.
///
/// # Errors
/// The statements'.
pub(crate) async fn before_drop(r: &mut dyn Runner, res: &Resolved) -> Result<()> {
    let pb = path_bits(&res.obj);
    if pb == 0 {
        return Ok(());
    }
    r.exec(&format!(
        "DELETE FROM {} WHERE `_sprig_state` = 'ovr' AND `_sprig_ovr` & {} = 0",
        res.layer, !pb
    ))
    .await?;
    r.exec(&format!(
        "UPDATE {} SET `_sprig_ovr` = `_sprig_ovr` & {} WHERE `_sprig_state` = 'ovr'",
        res.layer, !pb
    ))
    .await?;
    Ok(())
}

/// Refuses a hierarchical object that is received from a parent tenant or
/// shared to child tenants (decision T22, piece 7 isn't built).
///
/// # Errors
/// 39891.
pub(crate) fn refuse_tenants(ctx: &Ctx<'_>, o: &InheritableObject) -> Result<()> {
    if ctx.up_edge(o)?.is_some() {
        return Err(unsupported(
            "a record hierarchy on a table received from a parent tenant is not supported yet",
        ));
    }
    let reaches = ctx.state.shares_of(o.object_id).any(|sh| {
        matches!(sh.to, crate::model::ShareTarget::Tenant(_))
            || (sh.from == crate::model::ROOT_SPACE
                && matches!(sh.to, crate::model::ShareTarget::AllChildren))
    });
    if reaches {
        return Err(unsupported(
            "a record hierarchy on a table shared to child tenants is not supported yet",
        ));
    }
    Ok(())
}

/// Whether a share of `o` may be created or changed this way (decision
/// T22, piece 6): inside the tenant, without filters or column lists,
/// which would cut rows or the path columns out of a space's tree.
///
/// # Errors
/// 39891.
pub(crate) fn check_share(
    o: &InheritableObject,
    to_tenant: bool,
    all_children_from_root: bool,
    filtered: bool,
) -> Result<()> {
    if !has_paths(o) {
        return Ok(());
    }
    if to_tenant || all_children_from_root {
        return Err(unsupported(
            "a share of a table with a record hierarchy to child tenants (or ALL CHILDREN from \
             the root space) is not supported yet: share it to spaces",
        ));
    }
    if filtered {
        return Err(unsupported(
            "a share of a table with a record hierarchy can't have a filter, a column list or a \
             TAKE yet: a space would see rows without their parents",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
