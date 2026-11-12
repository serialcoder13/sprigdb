//! `LayerWriter` for tables (chapter 42, "Writes" and "Parent deletes and
//! orphans"; build steps 5 and 6).
//!
//! Every write targets the session's space `s` only:
//!
//! | Statement in `s` | Plan |
//! |---|---|
//! | `UPDATE` | read the matching keys from `view(o, s)`, then one `INSERT ... SELECT` from the view with `ON DUPLICATE KEY UPDATE`: an `ovr` row gains the `SET` columns' bits; `s`'s own rows are updated in place |
//! | `INSERT` | 1062 if the key is visible in `s`; else an `own` row, after removing `s`'s invisible `hide` and `ovr` rows of the key |
//! | `DELETE` | keys `s` inherits get a `hide` row; `s`'s own and orphan rows nobody above has are deleted; descendants' overrides that lose their record are orphaned (`keep`) or deleted (`follow`) in the same transaction |
//! | `SPRIG RESET` | deletes `s`'s `ovr`, `hide` and `orphan` rows, or clears the named fields' bits |
//!
//! When `s` inherits nothing and no space below it inherits `o` from it,
//! `UPDATE` and `DELETE` run as the client wrote them with `s`'s rows as
//! an extra condition, so MySQL's counts and `ORDER BY ... LIMIT` are
//! exact. Multi-statement plans run between `BEGIN` and `COMMIT`, or under
//! a savepoint inside the client's transaction.

use std::collections::{BTreeMap, BTreeSet};

use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::{Done, Rows, Txn};
use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, OnInsert, SetExpr, Statement, TableFactor, TableObject,
};

use crate::grammar::{Name, unsupported};
use crate::model::{ALL_BITS, InheritableObject, SpaceId};
use crate::plan::{Ctx, Resolved};
use crate::rewrite::{as_name, expr_text, parse_one, replace_refs};
use crate::view::{Chain, ViewSql, q};
use crate::{Handled, Runner, upstream};

fn tr(r: &mut dyn Runner) -> &mut dyn sprig_types::Runner {
    r
}

/// A text-protocol value as a SQL literal.
fn literal(rows: &Rows, row: &[Option<Vec<u8>>], i: usize, nbe: bool) -> String {
    match row.get(i).and_then(Option::as_ref) {
        None => "NULL".into(),
        Some(v) if rows.numeric(i) => String::from_utf8_lossy(v).into_owned(),
        Some(v) => match std::str::from_utf8(v) {
            Ok(s) => sprig_types::rewrite::quote_str(s, nbe),
            Err(_) => sprig_types::rewrite::hex_lit(v),
        },
    }
}

/// [`literal`] for other modules.
pub(crate) fn literal_of(rows: &Rows, row: &[Option<Vec<u8>>], i: usize, nbe: bool) -> String {
    literal(rows, row, i, nbe)
}

/// [`key_in`] for other modules.
pub(crate) fn key_in_sql(o: &InheritableObject, tuples: &[String]) -> String {
    key_in(o, tuples)
}

/// The key tuples of the first `n` columns of each row, as SQL.
fn key_tuples(rows: &Rows, n: usize, nbe: bool) -> Vec<String> {
    rows.rows
        .iter()
        .map(|row| {
            let parts: Vec<String> = (0..n).map(|i| literal(rows, row, i, nbe)).collect();
            if n == 1 {
                parts.into_iter().next().unwrap_or_default()
            } else {
                format!("({})", parts.join(", "))
            }
        })
        .collect()
}

fn key_list(o: &InheritableObject, alias: Option<&str>) -> String {
    o.key
        .iter()
        .map(|k| match alias {
            Some(a) => format!("{a}.{}", q(k)),
            None => q(k),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `key IN (tuples)` (never empty).
fn key_in(o: &InheritableObject, tuples: &[String]) -> String {
    if tuples.is_empty() {
        return "FALSE".into();
    }
    let lhs = if o.key.len() == 1 {
        q(&o.key[0])
    } else {
        format!("({})", key_list(o, None))
    };
    format!("{lhs} IN ({})", tuples.join(", "))
}

fn cols(o: &InheritableObject, alias: Option<&str>) -> String {
    o.columns
        .iter()
        .map(|c| match alias {
            Some(a) => format!("{a}.{}", q(c)),
            None => q(c),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn view(r: &Resolved, chain: &Chain, keys: Option<Vec<String>>) -> String {
    ViewSql {
        o: &r.obj,
        layer: r.layer.clone(),
        keys,
        origin: false,
        derived: r.derived.clone(),
    }
    .view(chain)
}

fn unknown_column(c: &str) -> SprigError {
    SprigError::standard(
        1054,
        *b"42S22",
        format!("Unknown column '{c}' in 'field list'"),
    )
}

fn key_update(c: &str) -> SprigError {
    SprigError::new(
        SprigCode::InheritKeyUpdate,
        format!("'{c}' is the inheritable key: a record's key can't change"),
    )
}

/// Maps 1062's entry text back to the client's key (the layer table's
/// key ends with `-<space>`).
#[must_use]
pub fn map_duplicate(mut e: SprigError) -> SprigError {
    let is_dup = matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1062, .. }
            | sprig_common::ErrorKind::Standard { errno: 1062, .. }
    );
    if !is_dup {
        return e;
    }
    if let Some(start) = e.message.find("Duplicate entry '")
        && let Some(end) = e.message.find("' for key '")
        && end > start + 17
    {
        let entry = &e.message[start + 17..end];
        if let Some((k, s)) = entry.rsplit_once('-')
            && !s.is_empty()
            && s.bytes().all(|b| b.is_ascii_digit())
        {
            let fixed = format!("{}{k}{}", &e.message[..start + 17], &e.message[end..]);
            e.message = fixed;
        }
    }
    e
}

fn duplicate(key: &str, index: &str) -> SprigError {
    SprigError::standard(
        1062,
        *b"23000",
        format!("Duplicate entry '{key}' for key '{index}'"),
    )
}

fn done(affected: u64) -> Done {
    Done {
        affected_rows: affected,
        ..Done::default()
    }
}

fn layer_op(op: &'static str) {
    metrics::counter!("sprig_inherit_layer_writes_total", "op" => op).increment(1);
}

/// Whether a space at or below `s` passes `o` on to a space below it
/// (inside the tenant): then a delete in `s` may orphan overrides.
fn feeds_below(ctx: &Ctx<'_>, o: &InheritableObject, s: SpaceId) -> bool {
    let below = ctx.state.descendants(s);
    ctx.state
        .shares_of(o.object_id)
        .any(|sh| sh.from == s || below.contains(&sh.from))
}

// ---------------------------------------------------------------- UPDATE

/// `UPDATE` (single-table) of an inheritable object; other updates only
/// read inheritable objects and are rewritten.
///
/// # Errors
/// 39886 for a key change; 39891 for multi-table forms and a `SET` that
/// reads a column an earlier assignment sets; the steps' errors.
pub async fn update(sql: &str, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let mut stmt = parse_one(sql)?;
    let Statement::Update(u) = &stmt else {
        return Ok(Handled::NotInherit);
    };
    let target = match &u.table.relation {
        TableFactor::Table { name, alias, .. } => as_name(name).and_then(|n| {
            ctx.resolve(&n)
                .ok()
                .flatten()
                .map(|r| (r, alias.clone(), n))
        }),
        _ => None,
    };
    let Some((res, alias, name)) = target else {
        // The target is an ordinary table: only reads to rewrite. A join
        // that names an inheritable object as a second target is refused
        // by MariaDB's rules on derived tables (1288).
        return Ok(match replace_refs(&mut stmt, ctx, r).await? {
            Some(t) => Handled::Forward(t),
            None => Handled::NotInherit,
        });
    };
    if !u.table.joins.is_empty() || u.from.is_some() {
        return Err(unsupported(
            "a multi-table UPDATE of an inheritable object is not supported yet",
        ));
    }
    let o = &res.obj;
    let a = q(&alias
        .as_ref()
        .map_or_else(|| name.name.clone(), |x| x.name.value.clone()));
    let mut assigned: Vec<(String, String)> = Vec::new();
    for asg in &u.assignments {
        let AssignmentTarget::ColumnName(c) = &asg.target else {
            return Err(unsupported("a tuple assignment on an inheritable object"));
        };
        let parts = crate::rewrite::name_parts(c);
        let col = parts.last().cloned().unwrap_or_default();
        let Some(col) = o.column(&col).map(str::to_owned) else {
            return Err(unknown_column(&col));
        };
        if o.is_key(&col) {
            return Err(key_update(&col));
        }
        let value = expr_text(&asg.value, ctx, r).await?;
        let reads = crate::grammar::identifiers(value.as_bytes(), &ctx.lex);
        if let Some((earlier, _)) = assigned
            .iter()
            .find(|(e, _)| reads.iter().any(|x| x.eq_ignore_ascii_case(e)))
        {
            return Err(unsupported(format!(
                "a SET value reading `{earlier}`, which the same statement sets, on an \
                 inheritable object: split the statement"
            )));
        }
        assigned.push((col, value));
    }
    let selection = match &u.selection {
        Some(e) => Some(expr_text(e, ctx, r).await?),
        None => None,
    };
    let order = if u.order_by.is_empty() {
        String::new()
    } else {
        format!(
            " ORDER BY {}",
            u.order_by
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let limit = u
        .limit
        .as_ref()
        .map(|l| format!(" LIMIT {l}"))
        .unwrap_or_default();
    let s = ctx.space;
    upstream::ensure(ctx, r, &res).await?;
    let chain = ctx.chain(o, s, &res.prefix)?;
    let nbe = ctx.lex.no_backslash_escapes;
    // Foreign keys whose columns this statement sets are checked after it.
    let sets_links = o.links.iter().any(|l| {
        l.columns
            .iter()
            .any(|c| assigned.iter().any(|(x, _)| x.eq_ignore_ascii_case(c)))
    });
    if chain.is_base() {
        // Only s's own rows: the client's statement with s as a condition.
        let sets: Vec<String> = assigned
            .iter()
            .map(|(c, v)| format!("{a}.{} = {v}", q(c)))
            .collect();
        let cond = format!(
            "{a}.`_sprig_space` = {} AND {a}.`_sprig_state` IN ('own', 'orphan'){}",
            s.0,
            selection
                .as_deref()
                .map(|w| format!(" AND ({w})"))
                .unwrap_or_default()
        );
        let text = format!(
            "UPDATE {} AS {a} SET {} WHERE {cond}{order}{limit}",
            res.layer,
            sets.join(", "),
        );
        layer_op("update");
        if !sets_links {
            return r.exec(&text).await.map(Handled::Ok).map_err(map_duplicate);
        }
        let tx = Txn::begin(tr(r)).await?;
        let result = async {
            let rows = r
                .query(&format!(
                    "SELECT {} FROM {} AS {a} WHERE {cond}{order}{limit}",
                    key_list(o, Some(&a)),
                    res.layer
                ))
                .await?;
            let keys = key_tuples(&rows, o.key.len(), nbe);
            let d = r.exec(&text).await.map_err(map_duplicate)?;
            crate::links::after_write(ctx, r, &res, &keys).await?;
            Ok(d)
        }
        .await;
        return tx.end(tr(r), result).await.map(Handled::Ok);
    }
    let same = if assigned.is_empty() {
        "TRUE".to_owned()
    } else {
        assigned
            .iter()
            .map(|(c, v)| format!("{a}.{} <=> ({v})", q(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let find = format!(
        "SELECT {}, ({same}) FROM ({}) AS {a}{}{order}{limit}",
        key_list(o, Some(&a)),
        view(&res, &chain, None),
        selection
            .as_deref()
            .map(|w| format!(" WHERE {w}"))
            .unwrap_or_default()
    );
    let tx = Txn::begin(tr(r)).await?;
    let result = async {
        let rows = r.query(&find).await?;
        let n = o.key.len();
        let keys = key_tuples(&rows, n, nbe);
        let matched = keys.len() as u64;
        let changed = rows
            .rows
            .iter()
            .filter(|row| {
                row.get(n)
                    .and_then(Option::as_ref)
                    .is_none_or(|v| v.as_slice() != b"1")
            })
            .count() as u64;
        if !keys.is_empty() && !assigned.is_empty() {
            let bits = if ctx.limits.override_record {
                ALL_BITS
            } else {
                assigned.iter().fold(0u64, |b, (c, _)| b | o.bit(c))
            };
            let list: Vec<String> = o
                .columns
                .iter()
                .map(|c| {
                    assigned
                        .iter()
                        .find(|(x, _)| x == c)
                        .map_or_else(|| format!("{a}.{}", q(c)), |(_, v)| format!("({v})"))
                })
                .collect();
            let upd: Vec<String> = o
                .values()
                .filter(|c| ctx.limits.override_record || assigned.iter().any(|(x, _)| x == c))
                .map(|c| format!("{c} = VALUES({c})", c = q(c)))
                .collect();
            let text = format!(
                "INSERT INTO {} ({}, `_sprig_space`, `_sprig_state`, `_sprig_ovr`) SELECT {}, {}, \
                 'ovr', {bits} FROM ({}) AS {a} ON DUPLICATE KEY UPDATE {}, `_sprig_ovr` = \
                 `_sprig_ovr` | {bits}",
                res.layer,
                cols(o, None),
                list.join(", "),
                s.0,
                view(&res, &chain, Some(keys.clone())),
                upd.join(", ")
            );
            r.exec(&text).await.map_err(map_duplicate)?;
            layer_op("override");
            if sets_links {
                crate::links::after_write(ctx, r, &res, &keys).await?;
            }
        }
        let affected = if r.found_rows() { matched } else { changed };
        Ok(Done {
            affected_rows: affected,
            info: format!("Rows matched: {matched}  Changed: {changed}  Warnings: 0"),
            ..Done::default()
        })
    }
    .await;
    tx.end(tr(r), result).await.map(Handled::Ok)
}

// ---------------------------------------------------------------- DELETE

/// `DELETE` (single-table) from an inheritable object.
///
/// # Errors
/// 39891 for multi-table forms; the steps' errors.
pub async fn delete(sql: &str, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let mut stmt = parse_one(sql)?;
    let Statement::Delete(d) = &stmt else {
        return Ok(Handled::NotInherit);
    };
    let from = match &d.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    let target = match from.first().map(|t| &t.relation) {
        Some(TableFactor::Table { name, alias, .. }) if d.tables.is_empty() => as_name(name)
            .and_then(|n| {
                ctx.resolve(&n)
                    .ok()
                    .flatten()
                    .map(|r| (r, alias.clone(), n))
            }),
        _ => None,
    };
    let multi_target = !d.tables.is_empty()
        && d.tables.iter().any(|t| {
            as_name(t).is_some_and(|n| {
                ctx.resolve(&n).ok().flatten().is_some()
                    || from.iter().any(|f| {
                        matches!(&f.relation, TableFactor::Table { name, alias: Some(al), .. }
                            if al.name.value.eq_ignore_ascii_case(&n.name)
                                && as_name(name).and_then(|x| ctx.resolve(&x).ok().flatten()).is_some())
                    })
            })
        });
    if multi_target {
        return Err(unsupported(
            "a multi-table DELETE from an inheritable object is not supported yet",
        ));
    }
    let Some((res, alias, name)) = target else {
        return Ok(match replace_refs(&mut stmt, ctx, r).await? {
            Some(t) => Handled::Forward(t),
            None => Handled::NotInherit,
        });
    };
    if from.len() > 1 || !from[0].joins.is_empty() || d.using.is_some() {
        return Err(unsupported(
            "a multi-table DELETE from an inheritable object is not supported yet",
        ));
    }
    let a = q(&alias
        .as_ref()
        .map_or_else(|| name.name.clone(), |x| x.name.value.clone()));
    let selection = match &d.selection {
        Some(e) => Some(expr_text(e, ctx, r).await?),
        None => None,
    };
    let order = if d.order_by.is_empty() {
        String::new()
    } else {
        format!(
            " ORDER BY {}",
            d.order_by
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let limit = d
        .limit
        .as_ref()
        .map(|l| format!(" LIMIT {l}"))
        .unwrap_or_default();
    let where_sql = selection
        .as_deref()
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    // Without an alias the client's condition can run on the table itself.
    let direct = if alias.is_none() {
        Direct::Yes(selection.as_deref())
    } else {
        Direct::No
    };
    delete_where(
        ctx,
        r,
        &res,
        &a,
        &where_sql,
        &format!("{order}{limit}"),
        direct,
    )
    .await
}

/// Deletes the records of `view(o, s)` that `where_sql` (with `tail`:
/// `ORDER BY`, `LIMIT`) selects. `direct`: the bare condition, when it may
/// run on the layer table itself.
async fn delete_where(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    a: &str,
    where_sql: &str,
    tail: &str,
    direct: Direct<'_>,
) -> Result<Handled> {
    let o = &res.obj;
    let s = ctx.space;
    upstream::ensure(ctx, r, res).await?;
    let chain = ctx.chain(o, s, &res.prefix)?;
    let below = feeds_below(ctx, o, s);
    let checked = crate::links::deletes_need_check(ctx, o);
    if let (true, false, false, Direct::Yes(cond)) = (chain.is_base(), below, checked, direct) {
        let text = format!(
            "DELETE FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('own', 'orphan'){}{tail}",
            res.layer,
            s.0,
            cond.map(|w| format!(" AND ({w})")).unwrap_or_default(),
        );
        layer_op("delete");
        return r.exec(&text).await.map(Handled::Ok);
    }
    let find = format!(
        "SELECT {} FROM ({}) AS {a}{where_sql}{tail}",
        key_list(o, Some(a)),
        view(res, &chain, None)
    );
    let tx = Txn::begin(tr(r)).await?;
    let result = async {
        let rows = r.query(&find).await?;
        let keys = key_tuples(&rows, o.key.len(), ctx.lex.no_backslash_escapes);
        if keys.is_empty() {
            return Ok(done(0));
        }
        let hexes = crate::links::row_hexes(&rows, o.key.len());
        delete_keys(ctx, r, res, &chain, &keys, &hexes, below).await?;
        Ok(done(keys.len() as u64))
    }
    .await;
    tx.end(tr(r), result).await.map(Handled::Ok)
}

/// Whether a delete's condition may run on the layer table itself.
#[derive(Debug, Clone, Copy)]
enum Direct<'a> {
    /// Yes, with this condition (`None`: every row).
    Yes(Option<&'a str>),
    /// No (the statement aliased the table).
    No,
}

/// A candidate for orphaning: a descendant's `ovr` row of a deleted key.
struct Candidate {
    key: String,
    space: SpaceId,
    depth: u16,
}

/// Deletes `keys` (visible in `s`) in `s`, and orphans what loses its
/// record below.
async fn delete_keys(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    chain: &Chain,
    keys: &[String],
    hexes: &[String],
    below: bool,
) -> Result<()> {
    let o = &res.obj;
    let s = ctx.space;
    let nbe = ctx.lex.no_backslash_escapes;
    let n = o.key.len();
    // Keys s inherits get a hide row; the rest are s's own.
    let inherited: BTreeSet<String> = match (ViewSql {
        o,
        layer: res.layer.clone(),
        keys: Some(keys.to_vec()),
        origin: false,
        derived: Vec::new(),
    })
    .inherited(chain)
    {
        Some(inh) => {
            let rows = r
                .query(&format!(
                    "SELECT {} FROM ({inh}) AS `__sprig_i`",
                    key_list(o, None)
                ))
                .await?;
            key_tuples(&rows, n, nbe).into_iter().collect()
        }
        None => BTreeSet::new(),
    };
    // Orphaning candidates and their parents' views before the change.
    let mut candidates = Vec::new();
    let mut before: BTreeMap<SpaceId, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    if below {
        let desc = ctx.state.descendants(s);
        if !desc.is_empty() {
            let rows = r
                .query(&format!(
                    "SELECT {}, `_sprig_space` FROM {} WHERE `_sprig_space` IN ({}) AND \
                     `_sprig_state` = 'ovr' AND {}",
                    key_list(o, None),
                    res.layer,
                    desc.iter()
                        .map(|d| d.0.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    key_in(o, keys)
                ))
                .await?;
            let tuples = key_tuples(&rows, n, nbe);
            for (row, key) in rows.rows.iter().zip(tuples) {
                let space = row
                    .get(n)
                    .and_then(Option::as_ref)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                    .map(SpaceId)
                    .ok_or_else(|| SprigError::internal("a layer row without a space"))?;
                let depth = ctx
                    .state
                    .space(space, ctx.tenant_name)
                    .map_or(0, |x| x.depth);
                candidates.push(Candidate { key, space, depth });
            }
        }
        let parents: BTreeSet<SpaceId> = candidates
            .iter()
            .filter_map(|c| ctx.state.parent(c.space))
            .collect();
        for p in parents {
            let ks: Vec<String> = candidates
                .iter()
                .filter(|c| ctx.state.parent(c.space) == Some(p))
                .map(|c| c.key.clone())
                .collect();
            let pc = ctx.chain(o, p, &res.prefix)?;
            let rows = r
                .query(&format!(
                    "SELECT {} FROM ({}) AS `__sprig_b`",
                    cols(o, None),
                    view(res, &pc, Some(ks))
                ))
                .await?;
            let key_pos: Vec<usize> = o
                .key
                .iter()
                .filter_map(|k| o.columns.iter().position(|c| c == k))
                .collect();
            let mut m = BTreeMap::new();
            for row in &rows.rows {
                let parts: Vec<String> = key_pos
                    .iter()
                    .map(|i| literal(&rows, row, *i, nbe))
                    .collect();
                let k = if n == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(", "))
                };
                let vals = (0..o.columns.len())
                    .map(|i| literal(&rows, row, i, nbe))
                    .collect();
                m.insert(k, vals);
            }
            before.insert(p, m);
        }
    }
    // Refused (1451) while a row of s's view, or of a space or tenant
    // below s, still references a key (Kiran's decision 4).
    let hidden: BTreeSet<String> = keys
        .iter()
        .zip(hexes)
        .filter(|(k, _)| inherited.contains(*k))
        .map(|(_, h)| h.clone())
        .collect();
    crate::links::before_delete(ctx, r, res, hexes, &hidden).await?;
    // Overrides of these rows in child tenants become orphans later (the
    // copy worker): their references are handed over until then.
    crate::links::hand_over(ctx, r, res, chain, keys).await?;
    // The change in s.
    let hide: Vec<String> = keys
        .iter()
        .filter(|k| inherited.contains(*k))
        .cloned()
        .collect();
    let gone: Vec<String> = keys
        .iter()
        .filter(|k| !inherited.contains(*k))
        .cloned()
        .collect();
    if !hide.is_empty() {
        r.exec(&format!(
            "INSERT INTO {} ({}, `_sprig_space`, `_sprig_state`, `_sprig_ovr`) SELECT {}, {}, \
             'hide', 0 FROM ({}) AS `__sprig_h` ON DUPLICATE KEY UPDATE `_sprig_state` = 'hide', \
             `_sprig_ovr` = 0",
            res.layer,
            cols(o, None),
            cols(o, Some("`__sprig_h`")),
            s.0,
            view(res, chain, Some(hide))
        ))
        .await?;
        layer_op("hide");
    }
    if !gone.is_empty() {
        r.exec(&format!(
            "DELETE FROM {} WHERE `_sprig_space` = {} AND {}",
            res.layer,
            s.0,
            key_in(o, &gone)
        ))
        .await?;
        layer_op("delete");
    }
    // Orphaning, top-down.
    candidates.sort_by_key(|c| c.depth);
    let depths: BTreeSet<u16> = candidates.iter().map(|c| c.depth).collect();
    for depth in depths {
        let level: Vec<&Candidate> = candidates.iter().filter(|c| c.depth == depth).collect();
        let parents: BTreeSet<SpaceId> = level
            .iter()
            .filter_map(|c| ctx.state.parent(c.space))
            .collect();
        for p in parents {
            let mine: Vec<&&Candidate> = level
                .iter()
                .filter(|c| ctx.state.parent(c.space) == Some(p))
                .collect();
            let ks: Vec<String> = mine.iter().map(|c| c.key.clone()).collect();
            let pc = ctx.chain(o, p, &res.prefix)?;
            let rows = r
                .query(&format!(
                    "SELECT {} FROM ({}) AS `__sprig_a`",
                    key_list(o, None),
                    view(res, &pc, Some(ks))
                ))
                .await?;
            let after: BTreeSet<String> = key_tuples(&rows, n, nbe).into_iter().collect();
            let was = before.get(&p);
            for c in mine {
                let Some(vals) = was.and_then(|m| m.get(&c.key)) else {
                    continue;
                };
                if after.contains(&c.key) {
                    continue;
                }
                let target = format!(
                    "`_sprig_space` = {} AND {}",
                    c.space.0,
                    key_in(o, std::slice::from_ref(&c.key))
                );
                if ctx.limits.parent_delete_follow {
                    r.exec(&format!("DELETE FROM {} WHERE {target}", res.layer))
                        .await?;
                    metrics::counter!("sprig_inherit_orphans_total", "policy" => "follow", "path" => "sync").increment(1);
                } else {
                    let sets: Vec<String> = o
                        .columns
                        .iter()
                        .zip(vals)
                        .filter(|(col, _)| !o.is_key(col))
                        .map(|(col, v)| {
                            format!(
                                "{c} = IF(`_sprig_ovr` & {b}, {c}, {v})",
                                c = q(col),
                                b = o.bit(col)
                            )
                        })
                        .collect();
                    r.exec(&format!(
                        "UPDATE {} SET {}{}`_sprig_state` = 'orphan', `_sprig_ovr` = {ALL_BITS} \
                         WHERE {target}",
                        res.layer,
                        sets.join(", "),
                        if sets.is_empty() { "" } else { ", " }
                    ))
                    .await?;
                    metrics::counter!("sprig_inherit_orphans_total", "policy" => "keep", "path" => "sync").increment(1);
                    if !o.links.is_empty() {
                        crate::links::claim_rows(
                            ctx,
                            r,
                            res,
                            c.space,
                            std::slice::from_ref(&c.key),
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- INSERT

/// `INSERT` and `REPLACE` into an inheritable object; other inserts only
/// read inheritable objects and are rewritten.
///
/// # Errors
/// 1062 for a visible key; 39891 for `INSERT ... SELECT`, `REPLACE` in a
/// space that inherits, and an upsert of an inherited record.
pub async fn insert(sql: &str, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let mut stmt = parse_one(sql)?;
    let target = match &stmt {
        Statement::Insert(ins) => match &ins.table {
            TableObject::TableName(n) => as_name(n).and_then(|n| ctx.resolve(&n).ok().flatten()),
            _ => None,
        },
        _ => return Ok(Handled::NotInherit),
    };
    let Some(res) = target else {
        return Ok(match replace_refs(&mut stmt, ctx, r).await? {
            Some(t) => Handled::Forward(t),
            None => Handled::NotInherit,
        });
    };
    let Statement::Insert(ins) = &stmt else {
        return Ok(Handled::NotInherit);
    };
    let o = &res.obj;
    let s = ctx.space;
    let nbe = ctx.lex.no_backslash_escapes;
    let mut columns: Vec<String> = ins
        .columns
        .iter()
        .map(|c| {
            crate::rewrite::name_parts(c)
                .last()
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    if !ins.assignments.is_empty() {
        columns = ins
            .assignments
            .iter()
            .map(|a| match &a.target {
                AssignmentTarget::ColumnName(c) => crate::rewrite::name_parts(c)
                    .last()
                    .cloned()
                    .unwrap_or_default(),
                AssignmentTarget::Tuple(_) => String::new(),
            })
            .collect();
    } else if columns.is_empty() {
        columns.clone_from(&o.columns);
    }
    for c in &columns {
        if o.column(c).is_none() {
            return Err(unknown_column(c));
        }
    }
    // The rows, as expressions.
    let mut rows: Vec<Vec<Expr>> = if ins.assignments.is_empty() {
        let Some(src) = &ins.source else {
            return Err(unsupported(
                "INSERT without VALUES into an inheritable object",
            ));
        };
        match src.body.as_ref() {
            SetExpr::Values(v) => v.rows.iter().map(|p| p.content.clone()).collect(),
            _ => {
                return Err(unsupported(
                    "INSERT ... SELECT into an inheritable object is not supported yet",
                ));
            }
        }
    } else {
        vec![ins.assignments.iter().map(|a| a.value.clone()).collect()]
    };
    // Generated ids come from the tree's one sequence (Kiran's decision 3).
    let learnt;
    let seq_obj: &InheritableObject = if ctx.state.ext_missing(o.object_id) {
        // Made inheritable before its AUTO_INCREMENT column was recorded.
        let cols = crate::guard::show_columns(r, &res.layer).await?;
        let mut x = (**o).clone();
        x.auto = crate::guard::auto_column(&cols);
        r.commit(&crate::store::InheritChange {
            upsert_objects: vec![x.clone()],
            ..crate::store::InheritChange::default()
        })
        .await?;
        learnt = x;
        &learnt
    } else {
        o
    };
    let generated = crate::ids::fill(ctx, r, seq_obj, &mut columns, &mut rows).await?;
    upstream::ensure(ctx, r, &res).await?;
    let chain = ctx.chain(o, s, &res.prefix)?;
    if ins.replace_into && chain.has_upstream() {
        return Err(unsupported(
            "REPLACE into an inheritable object from a space that inherits it",
        ));
    }
    let key_pos: Option<Vec<usize>> = o
        .key
        .iter()
        .map(|k| columns.iter().position(|c| c.eq_ignore_ascii_case(k)))
        .collect();
    let tuple = |row: &[Expr]| -> Option<String> {
        let pos = key_pos.as_ref()?;
        let parts: Vec<String> = pos
            .iter()
            .map(|i| row.get(*i).map(ToString::to_string))
            .collect::<Option<_>>()?;
        if parts
            .iter()
            .any(|p| p.eq_ignore_ascii_case("DEFAULT") || p.eq_ignore_ascii_case("NULL"))
        {
            return None;
        }
        Some(if parts.len() == 1 {
            parts[0].clone()
        } else {
            format!("({})", parts.join(", "))
        })
    };
    // Keys from the tree's sequence are new at every level: no visibility
    // check and no stale layer rows to clear (and no gap locks taken).
    let keys: Vec<String> = rows
        .iter()
        .enumerate()
        .filter(|(i, _)| !generated.contains(i))
        .filter_map(|(_, row)| tuple(row))
        .collect();
    let upsert = matches!(&ins.on, Some(OnInsert::DuplicateKeyUpdate(_)));
    let mut ignored = 0u64;
    let linked = !o.links.is_empty();
    let base = chain.is_base() && !linked;
    let tx = if base {
        None
    } else {
        Some(Txn::begin(tr(r)).await?)
    };
    let result = async {
        if !base && !keys.is_empty() {
            let vis = r
                .query(&format!(
                    "SELECT {}, CONCAT_WS('-', {}) FROM ({}) AS `__sprig_v`",
                    key_list(o, None),
                    key_list(o, None),
                    view(&res, &chain, Some(keys.clone()))
                ))
                .await?;
            let visible: BTreeMap<String, String> = key_tuples(&vis, o.key.len(), nbe)
                .into_iter()
                .zip(vis.rows.iter().map(|row| {
                    row.last()
                        .and_then(Option::as_ref)
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .unwrap_or_default()
                }))
                .collect();
            if !visible.is_empty() {
                if ins.ignore {
                    let before = rows.len();
                    rows.retain(|row| tuple(row).is_none_or(|k| !visible.contains_key(&k)));
                    ignored = (before - rows.len()) as u64;
                } else if upsert {
                    // Only s's own records may be upserted natively.
                    let ks: Vec<String> = visible.keys().cloned().collect();
                    let own = r
                        .query(&format!(
                            "SELECT {} FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN \
                             ('own', 'orphan') AND {}",
                            key_list(o, None),
                            res.layer,
                            s.0,
                            key_in(o, &ks)
                        ))
                        .await?;
                    let own: BTreeSet<String> =
                        key_tuples(&own, o.key.len(), nbe).into_iter().collect();
                    if ks.iter().any(|k| !own.contains(k)) {
                        return Err(unsupported(
                            "INSERT ... ON DUPLICATE KEY UPDATE of an inherited record: use \
                             UPDATE",
                        ));
                    }
                } else {
                    let (_, entry) = visible
                        .iter()
                        .next()
                        .map_or((String::new(), String::new()), |(k, v)| {
                            (k.clone(), v.clone())
                        });
                    return Err(duplicate(&entry, "PRIMARY"));
                }
            }
            if rows.is_empty() {
                return Ok(Done {
                    affected_rows: 0,
                    warnings: u16::try_from(ignored).unwrap_or(u16::MAX),
                    ..Done::default()
                });
            }
            // s's invisible hide and ovr rows of the new keys go.
            let fresh: Vec<String> = keys
                .iter()
                .filter(|k| !visible.contains_key(*k))
                .cloned()
                .collect();
            if !fresh.is_empty() {
                r.exec(&format!(
                    "DELETE FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('hide', \
                     'ovr') AND {}",
                    res.layer,
                    s.0,
                    key_in(o, &fresh)
                ))
                .await?;
            }
        }
        let text = insert_text(
            &mut stmt_with_rows(&stmt, &columns, &rows),
            &columns,
            s,
            &res,
        )?;
        let first = r.exec(&text).await;
        let mut d = match first {
            Ok(d) => d,
            Err(e) if base && is_dup(&e) && !keys.is_empty() => {
                // A stale hide or ovr row of s (a share was dropped): it
                // is invisible, so it goes, and the insert runs again.
                let n = r
                    .exec(&format!(
                        "DELETE FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('hide', \
                         'ovr') AND {}",
                        res.layer,
                        s.0,
                        key_in(o, &keys)
                    ))
                    .await?;
                if n.affected_rows == 0 {
                    return Err(map_duplicate(e));
                }
                r.exec(&text).await.map_err(map_duplicate)?
            }
            Err(e) => return Err(map_duplicate(e)),
        };
        d.warnings = d
            .warnings
            .saturating_add(u16::try_from(ignored).unwrap_or(u16::MAX));
        layer_op("add");
        if linked {
            let written: Vec<String> = rows.iter().filter_map(|row| tuple(row)).collect();
            crate::links::after_write(ctx, r, &res, &written).await?;
        }
        Ok(d)
    }
    .await;
    match tx {
        Some(t) => t.end(tr(r), result).await.map(Handled::Ok),
        None => result.map(Handled::Ok),
    }
}

fn is_dup(e: &SprigError) -> bool {
    matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. })
}

/// `stmt` (an `INSERT`) with its rows replaced (`columns`: the rows'
/// columns, which may have gained the `AUTO_INCREMENT` column).
fn stmt_with_rows(stmt: &Statement, columns: &[String], rows: &[Vec<Expr>]) -> Statement {
    let mut s = stmt.clone();
    if let Statement::Insert(ins) = &mut s {
        if !ins.assignments.is_empty() {
            if let Some(row) = rows.first() {
                for (a, v) in ins.assignments.iter_mut().zip(row) {
                    a.value = v.clone();
                }
                for (c, v) in columns.iter().zip(row).skip(ins.assignments.len()) {
                    ins.assignments.push(sqlparser::ast::Assignment {
                        target: AssignmentTarget::ColumnName(sqlparser::ast::ObjectName(vec![
                            sqlparser::ast::ObjectNamePart::Identifier(
                                sqlparser::ast::Ident::with_quote('`', c.clone()),
                            ),
                        ])),
                        value: v.clone(),
                    });
                }
            }
        } else if let Some(src) = &mut ins.source
            && let SetExpr::Values(v) = src.body.as_mut()
        {
            let template = v.rows.first().cloned();
            if let Some(t) = template {
                v.rows = rows
                    .iter()
                    .map(|r| {
                        let mut p = t.clone();
                        p.content.clone_from(r);
                        p
                    })
                    .collect();
            }
        }
    }
    s
}

/// The `INSERT` with `_sprig_space` added to the columns and every row.
fn insert_text(
    stmt: &mut Statement,
    columns: &[String],
    s: SpaceId,
    res: &Resolved,
) -> Result<String> {
    let Statement::Insert(ins) = stmt else {
        return Err(SprigError::internal("not an INSERT"));
    };
    let space = Expr::value(sqlparser::ast::Value::Number(s.0.to_string(), false));
    if ins.assignments.is_empty() {
        ins.columns = columns
            .iter()
            .chain(std::iter::once(&"_sprig_space".to_owned()))
            .map(|c| {
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::with_quote('`', c.clone()),
                )])
            })
            .collect();
        if let Some(src) = &mut ins.source
            && let SetExpr::Values(v) = src.body.as_mut()
        {
            for row in &mut v.rows {
                row.content.push(space.clone());
            }
        }
    } else {
        ins.assignments.push(sqlparser::ast::Assignment {
            target: AssignmentTarget::ColumnName(sqlparser::ast::ObjectName(vec![
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::with_quote(
                    '`',
                    "_sprig_space",
                )),
            ])),
            value: space,
        });
    }
    let _ = res;
    Ok(stmt.to_string())
}

// ----------------------------------------------------------- RESET, TRUNCATE

/// `SPRIG RESET t [(cols)] [WHERE e]` in the session's space.
///
/// # Errors
/// 39885 for an ordinary object; 39886 for a key column; 1054.
pub async fn reset(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    table: &Name,
    columns: Option<&[String]>,
    filter: Option<&str>,
) -> Result<Done> {
    let Some(res) = ctx.resolve(table)? else {
        return Err(SprigError::new(
            SprigCode::NotInheritable,
            format!("{} is not inheritable", table.name),
        ));
    };
    let o = &res.obj;
    let s = ctx.space;
    let cond = filter.map(|f| format!(" AND ({f})")).unwrap_or_default();
    layer_op("reset");
    let Some(cols) = columns else {
        // An orphan's record exists nowhere above: resetting it takes the
        // key out of this space's view, so it is checked like a delete
        // (1451 while referenced), and its references are handed over to
        // the overrides below that become orphans in turn.
        let check = crate::links::deletes_need_check(ctx, o)
            || (!o.links.is_empty() && crate::links::reaches_child_tenants(ctx, o));
        if check {
            let rows = r
                .query(&format!(
                    "SELECT {} FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` = \
                     'orphan'{cond}",
                    key_list(o, None),
                    res.layer,
                    s.0
                ))
                .await?;
            if !rows.rows.is_empty() {
                let keys = key_tuples(&rows, o.key.len(), ctx.lex.no_backslash_escapes);
                let hexes = crate::links::row_hexes(&rows, o.key.len());
                crate::links::before_delete(ctx, r, &res, &hexes, &BTreeSet::new()).await?;
                let chain = ctx.chain(o, s, &res.prefix)?;
                crate::links::hand_over(ctx, r, &res, &chain, &keys).await?;
            }
        }
        return r
            .exec(&format!(
                "DELETE FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('ovr', 'hide', \
                 'orphan'){cond}",
                res.layer, s.0
            ))
            .await;
    };
    let mut bits = 0u64;
    for c in cols {
        let Some(c) = o.column(c) else {
            return Err(unknown_column(c));
        };
        if o.is_key(c) {
            return Err(key_update(c));
        }
        bits |= o.bit(c);
    }
    let tx = Txn::begin(tr(r)).await?;
    let result = async {
        let d = r
            .exec(&format!(
                "UPDATE {} SET `_sprig_ovr` = `_sprig_ovr` & {} WHERE `_sprig_space` = {} AND \
                 `_sprig_state` = 'ovr'{cond}",
                res.layer, !bits, s.0
            ))
            .await?;
        r.exec(&format!(
            "DELETE FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` = 'ovr' AND \
             `_sprig_ovr` = 0{cond}",
            res.layer, s.0
        ))
        .await?;
        Ok(d)
    }
    .await;
    tx.end(tr(r), result).await
}

/// `TRUNCATE` of an inheritable object: the session space's own rows,
/// from a space that inherits nothing.
///
/// # Errors
/// 39895 while shared; 39891 from a space that inherits the object.
pub async fn truncate(table: &Name, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let Some(res) = ctx.resolve(table)? else {
        return Ok(Handled::NotInherit);
    };
    crate::guard::refuse_if_shared_now(ctx, r, &res.obj, "TRUNCATE").await?;
    if crate::links::deletes_need_check(ctx, &res.obj) {
        return Err(unsupported(
            "TRUNCATE of an inheritable table other tables reference: use DELETE, which checks \
             the references",
        ));
    }
    let chain = ctx.chain(&res.obj, ctx.space, &res.prefix)?;
    if chain.has_upstream() {
        return Err(unsupported(
            "TRUNCATE of an inheritable object from a space that inherits it",
        ));
    }
    r.exec(&format!(
        "DELETE FROM {} WHERE `_sprig_space` = {}",
        res.layer, ctx.space.0
    ))
    .await?;
    Ok(Handled::Ok(Done::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_entries_lose_the_space() {
        let e = SprigError::backend(1062, *b"23000", "Duplicate entry '5-12' for key 'PRIMARY'");
        assert_eq!(
            map_duplicate(e).message,
            "Duplicate entry '5' for key 'PRIMARY'"
        );
        let e = SprigError::backend(1062, *b"23000", "Duplicate entry 'a-b' for key 'uk'");
        assert_eq!(
            map_duplicate(e).message,
            "Duplicate entry 'a-b' for key 'uk'"
        );
    }
}
