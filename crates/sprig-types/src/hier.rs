//! Record hierarchies (chapter 32, "Hierarchy columns", "Maintaining
//! paths", "Moving a subtree", "Declaring a hierarchy on existing rows";
//! build steps 10 and 11, single shard).
//!
//! A table with a hierarchy has three invisible columns, `_sprig_hpath`
//! (`/1/7/42/`), `_sprig_hdepth` (0 at the top) and `_sprig_rk` (the top
//! ancestor's key), and an index on the path. The gateway keeps them:
//!
//! - **Insert.** The client's `INSERT` runs as written (any form: several
//!   rows, `INSERT ... SELECT`, auto-increment keys), then, in the same
//!   transaction, the rows without a path get one, level by level: a row
//!   whose parent has a path takes the parent's path plus its own segment.
//!   A row still without a path has a missing parent (39712) or is too deep
//!   or in a cycle (39713), and the statement rolls back.
//! - **Update.** Changing the key is refused (39706). Changing the parent
//!   moves the subtree: one range `UPDATE` rewrites every path under the
//!   moved row, after checking cycles (39711), size (39715) and depth
//!   (39713), with the moved tree's top rows locked.
//! - **Delete.** The rows are found first; under `RESTRICT` a remaining
//!   child fails the statement (39714); under `CASCADE` the subtrees go
//!   too, deepest first.
//!
//! The path columns are nullable: rows written around the gateway have no
//! path until a gateway write or `ADD HIERARCHY` fills it.

use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sqlparser::ast::{AssignmentTarget, Delete, FromTable, Insert, Update};

use crate::Ctx;
use crate::exec::{Done, Rows, Runner, Txn, unsupported};
use crate::grammar::{AddHierarchy, Name};
use crate::model::{HierState, HierarchyDef, KeyClass, OnDelete, upper};
use crate::rewrite::{hex_lit, key_literal, segment, segment_sql};
use crate::store::TypesChange;
use crate::tabledef::TableDef;
use crate::write::WriteOutcome;

/// The path column.
pub const HPATH: &str = "_sprig_hpath";
/// The depth column.
pub const HDEPTH: &str = "_sprig_hdepth";
/// The top ancestor's key.
pub const RK: &str = "_sprig_rk";
/// The longest path, in bytes.
pub const MAX_PATH_BYTES: usize = 2048;

fn table_sql(db: Option<&str>, t: &str) -> String {
    match db {
        Some(d) => format!("{}.{}", quote(d), quote(t)),
        None => quote(t),
    }
}

fn val(rows: &Rows, row: usize, col: usize) -> Option<Vec<u8>> {
    rows.rows.get(row)?.get(col)?.clone()
}

fn num(rows: &Rows, row: usize, col: usize) -> Option<i64> {
    std::str::from_utf8(&val(rows, row, col)?)
        .ok()?
        .parse()
        .ok()
}

fn text_of(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

/// Gives every row without a path one, level by level (at most `MAX
/// DEPTH` + 1 rounds), and checks none is left.
///
/// # Errors
/// 39712, 39713, or a statement's error.
pub async fn fill_paths(h: &HierarchyDef, db: Option<&str>, r: &mut dyn Runner) -> Result<u64> {
    let t = table_sql(db, &h.table);
    let k = quote(&h.key);
    let p = quote(&h.parent);
    let mut filled = r
        .exec(&format!(
            "UPDATE {t} SET {HPATH} = CONCAT('/', {seg}, '/'), {HDEPTH} = 0, {RK} = {k} \
             WHERE {HPATH} IS NULL AND {p} IS NULL",
            seg = segment_sql(&k, h.key_class)
        ))
        .await?
        .affected_rows;
    for _ in 0..=h.max_depth {
        let n = r
            .exec(&format!(
                "UPDATE {t} AS `c` JOIN {t} AS `p` ON `c`.{p} = `p`.{k} \
                 SET `c`.{HPATH} = CONCAT(`p`.{HPATH}, {seg}, '/'), \
                 `c`.{HDEPTH} = `p`.{HDEPTH} + 1, `c`.{RK} = `p`.{RK} \
                 WHERE `c`.{HPATH} IS NULL AND `p`.{HPATH} IS NOT NULL AND `p`.{HDEPTH} < {max} \
                 AND LENGTH(`p`.{HPATH}) + LENGTH({seg}) + 1 <= {MAX_PATH_BYTES}",
                seg = segment_sql(&format!("`c`.{k}"), h.key_class),
                max = h.max_depth
            ))
            .await?
            .affected_rows;
        filled += n;
        if n == 0 {
            break;
        }
    }
    Ok(filled)
}

/// The first row without a path, if any, as an error.
async fn check_paths(
    h: &HierarchyDef,
    db: Option<&str>,
    r: &mut dyn Runner,
    nbe: bool,
) -> Result<()> {
    let t = table_sql(db, &h.table);
    let k = quote(&h.key);
    let p = quote(&h.parent);
    let left = r
        .query(&format!(
            "SELECT `c`.{k}, `c`.{p}, `p`.{k} IS NOT NULL FROM {t} AS `c` \
             LEFT JOIN {t} AS `p` ON `p`.{k} = `c`.{p} WHERE `c`.{HPATH} IS NULL LIMIT 1"
        ))
        .await?;
    if left.rows.is_empty() {
        return Ok(());
    }
    let key = key_literal(&val(&left, 0, 0).unwrap_or_default(), h.key_class, nbe);
    let parent = key_literal(&val(&left, 0, 1).unwrap_or_default(), h.key_class, nbe);
    if num(&left, 0, 2) == Some(1) {
        return Err(SprigError::new(
            SprigCode::HierarchyTooDeep,
            format!(
                "Record {key} of '{}' would be deeper than MAX DEPTH {} (or its parents form a \
                 cycle)",
                h.table, h.max_depth
            ),
        ));
    }
    Err(SprigError::new(
        SprigCode::HierarchyParentNotFound,
        format!(
            "Parent {parent} of record {key} doesn't exist in '{}'",
            h.table
        ),
    ))
}

/// An `INSERT` into a table with a hierarchy.
///
/// # Errors
/// 39710 for `REPLACE` and upserts changing the parent; [`fill_paths`]'s.
pub async fn insert(
    ins: &Insert,
    sql: &str,
    h: &HierarchyDef,
    db: Option<&str>,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<WriteOutcome> {
    if ins.replace_into {
        return Err(unsupported(
            "REPLACE into a table with a hierarchy is not supported yet",
        ));
    }
    if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(a)) = &ins.on
        && a.iter().any(|a| {
            matches!(&a.target, AssignmentTarget::ColumnName(c)
            if c.to_string().trim_matches('`').eq_ignore_ascii_case(&h.parent)
                || c.to_string().trim_matches('`').eq_ignore_ascii_case(&h.key))
        })
    {
        return Err(unsupported(
            "ON DUPLICATE KEY UPDATE of a hierarchy's key or parent is not supported yet",
        ));
    }
    let tx = Txn::begin(r).await?;
    let result: Result<Done> = async {
        let done = r.exec(sql).await?;
        fill_paths(h, db, r).await?;
        check_paths(h, db, r, ctx.lex.no_backslash_escapes).await?;
        Ok(done)
    }
    .await;
    let done = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "insert", "plan" => "hierarchy")
        .increment(1);
    Ok(WriteOutcome::Done(done))
}

/// An `UPDATE` of a table with a hierarchy.
///
/// # Errors
/// 39706, 39711, 39712, 39713, 39715, or a statement's error.
pub async fn update(
    upd: &Update,
    sql: &str,
    h: &HierarchyDef,
    db: Option<&str>,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<WriteOutcome> {
    let mut new_parent = None;
    for a in &upd.assignments {
        let AssignmentTarget::ColumnName(c) = &a.target else {
            return Err(unsupported(
                "a tuple assignment on a table with a hierarchy",
            ));
        };
        let c =
            c.0.last()
                .map(|p| p.to_string().trim_matches('`').to_owned())
                .unwrap_or_default();
        if c.eq_ignore_ascii_case(&h.key) {
            return Err(SprigError::new(
                SprigCode::TypeKeyUpdate,
                format!("The key '{c}' of a record in a hierarchy can't change"),
            ));
        }
        if c.eq_ignore_ascii_case(&h.parent) {
            new_parent = Some(a.value.clone());
        }
    }
    let Some(new_parent) = new_parent else {
        return Ok(WriteOutcome::NotTyped);
    };
    if !upd.table.joins.is_empty() || upd.from.is_some() {
        return Err(unsupported("a multi-table UPDATE of a hierarchy's parent"));
    }
    let nbe = ctx.lex.no_backslash_escapes;
    let mut step_a = format!(
        "SELECT {k}, {p}, {new_parent} FROM {from}",
        k = quote(&h.key),
        p = quote(&h.parent),
        from = upd.table
    );
    if let Some(w) = &upd.selection {
        step_a.push_str(&format!(" WHERE {w}"));
    }
    if !upd.order_by.is_empty() {
        let o: Vec<String> = upd.order_by.iter().map(ToString::to_string).collect();
        step_a.push_str(&format!(" ORDER BY {}", o.join(", ")));
    }
    if let Some(l) = &upd.limit {
        step_a.push_str(&format!(" LIMIT {l}"));
    }
    step_a.push_str(" FOR UPDATE");
    let tx = Txn::begin(r).await?;
    let result: Result<Done> = async {
        let found = r.query(&step_a).await?;
        let done = r.exec(sql).await?;
        for i in 0..found.rows.len() {
            let old = val(&found, i, 1);
            let new = val(&found, i, 2);
            if old == new {
                continue;
            }
            let node = val(&found, i, 0).unwrap_or_default();
            move_subtree(h, db, &node, new.as_deref(), ctx, r, nbe).await?;
        }
        Ok(done)
    }
    .await;
    let done = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "update", "plan" => "hierarchy")
        .increment(1);
    Ok(WriteOutcome::Done(done))
}

/// Moves `node`'s subtree under `new_parent` (`None`: to the top).
///
/// # Errors
/// 39711, 39712, 39713, 39715.
pub async fn move_subtree(
    h: &HierarchyDef,
    db: Option<&str>,
    node: &[u8],
    new_parent: Option<&[u8]>,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    nbe: bool,
) -> Result<()> {
    let t = table_sql(db, &h.table);
    let k = quote(&h.key);
    let n_lit = key_literal(node, h.key_class, nbe);
    let n = r
        .query(&format!(
            "SELECT {HPATH}, {HDEPTH}, {RK} FROM {t} WHERE {k} = {n_lit} FOR UPDATE"
        ))
        .await?;
    let Some(old) = val(&n, 0, 0) else {
        // A row without a path: the fill below gives it one.
        fill_paths(h, db, r).await?;
        return check_paths(h, db, r, nbe).await;
    };
    let n_depth = num(&n, 0, 1).unwrap_or(0);
    let n_rk = val(&n, 0, 2).unwrap_or_default();
    let seg = segment(node, h.key_class);
    let (new_path, new_depth, new_rk) = match new_parent {
        None => (format!("/{seg}/").into_bytes(), 0i64, node.to_vec()),
        Some(p) => {
            let p_lit = key_literal(p, h.key_class, nbe);
            let pr = r
                .query(&format!(
                    "SELECT {HPATH}, {HDEPTH}, {RK} FROM {t} WHERE {k} = {p_lit} FOR UPDATE"
                ))
                .await?;
            let Some(pp) = val(&pr, 0, 0) else {
                return Err(SprigError::new(
                    SprigCode::HierarchyParentNotFound,
                    format!(
                        "Parent {p_lit} of record {n_lit} doesn't exist in '{}'",
                        h.table
                    ),
                ));
            };
            if pp.starts_with(&old) {
                return Err(SprigError::new(
                    SprigCode::HierarchyCycle,
                    format!("Record {n_lit} can't move under its own descendant {p_lit}"),
                ));
            }
            let mut np = pp.clone();
            np.extend_from_slice(seg.as_bytes());
            np.push(b'/');
            (
                np,
                num(&pr, 0, 1).unwrap_or(0) + 1,
                val(&pr, 0, 2).unwrap_or_default(),
            )
        }
    };
    // One structural change per tree at a time: lock both top rows, in
    // key order.
    let mut tops = vec![
        key_literal(&n_rk, h.key_class, nbe),
        key_literal(&new_rk, h.key_class, nbe),
    ];
    tops.dedup();
    r.query(&format!(
        "SELECT {k} FROM {t} WHERE {k} IN ({}) ORDER BY {k} FOR UPDATE",
        tops.join(", ")
    ))
    .await?;
    let range = format!(
        "{HPATH} >= {} AND {HPATH} < {}",
        hex_lit(&old),
        hex_lit(&upper(&old))
    );
    let size = r
        .query(&format!(
            "SELECT COUNT(*), MAX({HDEPTH}), MAX(LENGTH({HPATH})) FROM {t} WHERE {range}"
        ))
        .await?;
    let count = num(&size, 0, 0).unwrap_or(0);
    if u64::try_from(count).unwrap_or(0) > ctx.limits.max_move_rows {
        return Err(SprigError::new(
            SprigCode::SubtreeTooLarge,
            format!(
                "Moving record {n_lit} moves {count} rows, over hierarchy.max_move_rows ({})",
                ctx.limits.max_move_rows
            ),
        ));
    }
    let delta = new_depth - n_depth;
    let deepest = num(&size, 0, 1).unwrap_or(n_depth) + delta;
    let longest = num(&size, 0, 2).unwrap_or(0) - i64::try_from(old.len()).unwrap_or(0)
        + i64::try_from(new_path.len()).unwrap_or(0);
    if deepest > i64::from(h.max_depth)
        || longest > i64::try_from(MAX_PATH_BYTES).unwrap_or(i64::MAX)
    {
        return Err(SprigError::new(
            SprigCode::HierarchyTooDeep,
            format!(
                "Moving record {n_lit} would make '{}' deeper than MAX DEPTH {} or its paths \
                 longer than {MAX_PATH_BYTES} bytes",
                h.table, h.max_depth
            ),
        ));
    }
    r.exec(&format!(
        "UPDATE {t} SET {HPATH} = CONCAT({np}, SUBSTRING({HPATH}, {len})), \
         {HDEPTH} = {HDEPTH} + ({delta}), {RK} = {rk} WHERE {range}",
        np = hex_lit(&new_path),
        len = old.len() + 1,
        rk = key_literal(&new_rk, h.key_class, nbe)
    ))
    .await?;
    metrics::counter!("sprig_hierarchy_moves_total", "scope" => "local").increment(1);
    #[allow(clippy::cast_precision_loss)]
    metrics::histogram!("sprig_hierarchy_move_size_rows").record(count as f64);
    Ok(())
}

/// A `DELETE` from a table with a hierarchy.
///
/// # Errors
/// 39714, 39715, or a statement's error.
pub async fn delete(
    del: &Delete,
    h: &HierarchyDef,
    db: Option<&str>,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<WriteOutcome> {
    let from = match &del.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    if from.len() != 1 || !from[0].joins.is_empty() || !del.tables.is_empty() || del.using.is_some()
    {
        return Err(unsupported(
            "a multi-table DELETE from a table with a hierarchy",
        ));
    }
    let nbe = ctx.lex.no_backslash_escapes;
    let t = table_sql(db, &h.table);
    let k = quote(&h.key);
    let mut step_a = format!("SELECT {k}, {HPATH} FROM {}", from[0]);
    if let Some(w) = &del.selection {
        step_a.push_str(&format!(" WHERE {w}"));
    }
    if !del.order_by.is_empty() {
        let o: Vec<String> = del.order_by.iter().map(ToString::to_string).collect();
        step_a.push_str(&format!(" ORDER BY {}", o.join(", ")));
    }
    if let Some(l) = &del.limit {
        step_a.push_str(&format!(" LIMIT {l}"));
    }
    step_a.push_str(" FOR UPDATE");
    let batch = ctx.limits.write_batch_rows.max(1);
    let max_move = ctx.limits.max_move_rows;
    let tx = Txn::begin(r).await?;
    let result: Result<u64> = async {
        let found = r.query(&step_a).await?;
        let keys: Vec<String> = (0..found.rows.len())
            .map(|i| key_literal(&val(&found, i, 0).unwrap_or_default(), h.key_class, nbe))
            .collect();
        let paths: Vec<Vec<u8>> = (0..found.rows.len())
            .filter_map(|i| val(&found, i, 1))
            .collect();
        let mut n = 0;
        for chunk in keys.chunks(batch) {
            n += r
                .exec(&format!(
                    "DELETE FROM {t} WHERE {k} IN ({})",
                    chunk.join(", ")
                ))
                .await?
                .affected_rows;
        }
        for chunk in paths.chunks(batch) {
            let under: Vec<String> = chunk
                .iter()
                .map(|p| {
                    format!(
                        "({HPATH} > {} AND {HPATH} < {})",
                        hex_lit(p),
                        hex_lit(&upper(p))
                    )
                })
                .collect();
            let cond = under.join(" OR ");
            match h.on_delete {
                OnDelete::Restrict => {
                    let child = r
                        .query(&format!("SELECT {k} FROM {t} WHERE {cond} LIMIT 1"))
                        .await?;
                    if let Some(c) = val(&child, 0, 0) {
                        return Err(SprigError::new(
                            SprigCode::HierarchyHasChildren,
                            format!(
                                "Cannot delete a record of '{}' that has children (record {} \
                                 is under it): delete them first, or declare ON DELETE CASCADE",
                                h.table,
                                key_literal(&c, h.key_class, nbe)
                            ),
                        ));
                    }
                }
                OnDelete::Cascade => {
                    let c = r
                        .query(&format!("SELECT COUNT(*) FROM {t} WHERE {cond}"))
                        .await?
                        .scalar_u64()
                        .unwrap_or(0);
                    if c > max_move {
                        return Err(SprigError::new(
                            SprigCode::SubtreeTooLarge,
                            format!(
                                "The cascade deletes {c} rows, over hierarchy.max_move_rows \
                                 ({max_move})"
                            ),
                        ));
                    }
                    r.exec(&format!(
                        "DELETE FROM {t} WHERE {cond} ORDER BY {HDEPTH} DESC"
                    ))
                    .await?;
                }
            }
        }
        Ok(n)
    }
    .await;
    let n = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "delete", "plan" => "hierarchy")
        .increment(1);
    Ok(WriteOutcome::Done(Done {
        affected_rows: n,
        ..Done::default()
    }))
}

/// `ALTER TABLE t ADD HIERARCHY (parent) ...`: the columns, the catalog
/// row, the backfill.
///
/// # Errors
/// 39710 for typed tables, composite keys or key types without segments,
/// tables that have a hierarchy; 1054 for an unknown parent column; 39716
/// when rows are left without a path.
pub async fn add(ah: &AddHierarchy, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Done> {
    let db = ah.table.db.as_deref();
    if let Some(d) = ctx.lookup.db(db) {
        if d.get(&ah.table.name, ctx.lookup.lctn()).is_some() {
            return Err(unsupported(
                "a record hierarchy on a typed table is not supported yet",
            ));
        }
        if d.hierarchy(&ah.table.name, ctx.lookup.lctn()).is_some() {
            return Err(unsupported(format!(
                "table '{}' already has a hierarchy (one per table)",
                ah.table.name
            )));
        }
    }
    let t = table_sql(db, &ah.table.name);
    let shown = r.query(&format!("SHOW CREATE TABLE {t}")).await?;
    let text = val(&shown, 0, 1).map(|v| text_of(&v)).unwrap_or_default();
    let def = TableDef::parse(&text, &ctx.lex)
        .ok_or_else(|| SprigError::internal("SHOW CREATE TABLE didn't parse"))?;
    let pk = def.primary_key();
    if pk.len() != 1 || ah.parent.len() != 1 {
        return Err(unsupported(
            "a hierarchy needs a one-column primary key and one parent column",
        ));
    }
    let key = def
        .column(&pk[0])
        .ok_or_else(|| SprigError::internal("primary key column missing"))?;
    let Some(class) = KeyClass::of_type(&key.data_type) else {
        return Err(unsupported(format!(
            "a hierarchy keyed by a {} column",
            key.data_type
        )));
    };
    let parent = def.column(&ah.parent[0]).ok_or_else(|| {
        SprigError::standard(
            1054,
            *b"42S22",
            format!("Unknown column '{}' in 'ADD HIERARCHY'", ah.parent[0]),
        )
    })?;
    let rk_type = key
        .data_type
        .split_whitespace()
        .filter(|w| !w.eq_ignore_ascii_case("zerofill"))
        .collect::<Vec<_>>()
        .join(" ");
    r.exec(&format!(
        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {HPATH} VARBINARY({MAX_PATH_BYTES}) NULL INVISIBLE, \
         ADD COLUMN IF NOT EXISTS {HDEPTH} SMALLINT UNSIGNED NULL INVISIBLE, \
         ADD COLUMN IF NOT EXISTS {RK} {rk_type} NULL INVISIBLE, \
         ADD INDEX IF NOT EXISTS {HPATH} ({HPATH})"
    ))
    .await?;
    let (db_id, object_id) = r
        .object_id(db, &ah.table.name)
        .await?
        .ok_or_else(|| SprigError::internal("the table vanished during ADD HIERARCHY"))?;
    let mut h = HierarchyDef {
        object_id,
        db: db_id,
        table: ah.table.name.clone(),
        key: key.name.clone(),
        key_class: class,
        key_type: rk_type,
        parent: parent.name.clone(),
        on_delete: if ah.cascade {
            OnDelete::Cascade
        } else {
            OnDelete::Restrict
        },
        max_depth: ah.max_depth.unwrap_or(ctx.limits.hierarchy_max_depth),
        state: HierState::Backfilling,
    };
    r.commit_types(&TypesChange {
        upsert_hierarchies: vec![h.clone()],
        ..TypesChange::default()
    })
    .await?;
    let filled = fill_paths(&h, db, r).await?;
    metrics::counter!("sprig_hierarchy_backfill_rows_total").increment(filled);
    let k = quote(&h.key);
    let orphans = r
        .query(&format!(
            "SELECT {k} FROM {t} WHERE {HPATH} IS NULL LIMIT 100"
        ))
        .await?;
    if !orphans.rows.is_empty() {
        let keys: Vec<String> = (0..orphans.rows.len())
            .map(|i| {
                key_literal(
                    &val(&orphans, i, 0).unwrap_or_default(),
                    h.key_class,
                    ctx.lex.no_backslash_escapes,
                )
            })
            .collect();
        drop_columns(&t, r).await?;
        r.commit_types(&TypesChange {
            delete_hierarchies: vec![h.object_id],
            ..TypesChange::default()
        })
        .await?;
        return Err(SprigError::new(
            SprigCode::HierarchyBackfillFailed,
            format!(
                "ADD HIERARCHY on '{}' failed: these records have a missing parent, a cycle, or \
                 a depth over MAX DEPTH {}: {}",
                h.table,
                h.max_depth,
                keys.join(", ")
            ),
        ));
    }
    h.state = HierState::Active;
    r.commit_types(&TypesChange {
        upsert_hierarchies: vec![h],
        ..TypesChange::default()
    })
    .await?;
    Ok(Done::default())
}

async fn drop_columns(t: &str, r: &mut dyn Runner) -> Result<()> {
    r.exec(&format!(
        "ALTER TABLE {t} DROP COLUMN IF EXISTS {HPATH}, DROP COLUMN IF EXISTS {HDEPTH}, \
         DROP COLUMN IF EXISTS {RK}"
    ))
    .await?;
    Ok(())
}

/// `ALTER TABLE t DROP HIERARCHY`.
///
/// # Errors
/// 39710 if the table has none.
pub async fn drop(name: &Name, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Done> {
    let h = ctx
        .lookup
        .db(name.db.as_deref())
        .and_then(|d| d.hierarchy(&name.name, ctx.lookup.lctn()).cloned())
        .ok_or_else(|| unsupported(format!("table '{}' has no hierarchy", name.name)))?;
    let mut dropping = (*h).clone();
    dropping.state = HierState::Dropping;
    r.commit_types(&TypesChange {
        upsert_hierarchies: vec![dropping],
        ..TypesChange::default()
    })
    .await?;
    drop_columns(&table_sql(name.db.as_deref(), &name.name), r).await?;
    r.commit_types(&TypesChange {
        delete_hierarchies: vec![h.object_id],
        ..TypesChange::default()
    })
    .await?;
    Ok(Done::default())
}
