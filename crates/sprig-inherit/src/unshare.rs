//! What a parent tenant's `DROP SHARE` does to the children the share
//! served (bugfix-44; the lead's choice of 12 November 2026, chapter 42,
//! "Parent deletes and orphans").
//!
//! After the share's catalog change commits, for every tenant edge the
//! share carried (its table, and the tables of its foreign-key group
//! through derived shares):
//!
//! ```text
//! each direct child the share served:
//!   DROP VIEW __sprig_share_<id> in the child's schema
//!   no other share of the table serves it now →
//!     its overrides (`ovr` rows of its root space) become `orphan` rows
//!     with every bit set: the child's own records, as they were written
//! each tenant below such a child:
//!   its overrides of records nobody above it (from that child down) still
//!   holds become `orphan` rows too, as the copy worker's orphan pass does
//! ```
//!
//! With the overrides, the records a child had hidden (`hide` rows) go:
//! they hid records of a share that no longer exists, and a later share
//! of the table would otherwise come with them still hidden. Below a cut
//! child, a grandchild's hides of records nobody from that child down
//! still holds go too. A materialized edge's copy (`__sprig_ih_<object>`)
//! and its state row go with the view; the copy worker only emptied it
//! (bugfix-55).
//!
//! Before, a child's override was hidden from the moment of the drop (its
//! level-0 view no longer had an upstream, and reads `own` and `orphan`
//! rows only), which looked like data loss, and the share view stayed in
//! the child's schema, pointing at the parent's dropped table, until the
//! child dropped the database.
//!
//! The work runs as the provisioning account on each tenant's cluster.
//! The share is already dropped when it runs, so a failure isn't
//! returned: every step can run again, and the drop runs the whole
//! cleanup up to [`ATTEMPTS`] times ([`Retry`], bugfix-55) before it
//! logs and counts the failure; the overrides then stay hidden (the
//! behaviour before).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use sprig_catalog::ddl::database_key;
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};

use crate::Runner;
use crate::copy::{STATE_TABLE, UPSTREAM, copy_table, key_text};
use crate::ids::TreeTable;
use crate::model::{ALL_BITS, ROOT_SPACE, Share, ShareTarget};
use crate::plan::{Ctx, state_of};
use crate::view::q;

/// Keys per `IN (...)` list.
const CHUNK: usize = 500;

/// How many times a drop runs its cleanup before it gives up.
pub const ATTEMPTS: usize = 3;

/// The pause before each attempt after the first.
const RETRY_PAUSE: [Duration; 2] = [Duration::from_millis(200), Duration::from_secs(1)];

/// The retry policy of a drop's cleanup: [`Retry::again`] after each
/// failed attempt says whether to run it again, and after which pause.
#[derive(Debug, Default)]
pub struct Retry {
    attempts: usize,
}

impl Retry {
    /// After a failed attempt: the pause before the next one, or `None`
    /// once [`ATTEMPTS`] have run.
    pub fn again(&mut self) -> Option<Duration> {
        self.attempts += 1;
        (self.attempts < ATTEMPTS)
            .then(|| RETRY_PAUSE[(self.attempts - 1).min(RETRY_PAUSE.len() - 1)])
    }

    /// Attempts that failed so far.
    #[must_use]
    pub fn failed(&self) -> usize {
        self.attempts
    }
}

/// One tenant edge a dropped share carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Carried {
    /// The parent's object.
    pub object: ObjectId,
    /// The share's id (a derived share's for a foreign-key group table).
    pub share: u64,
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
}

/// What [`after_drop`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unshared {
    /// Share views dropped (`DROP VIEW IF EXISTS` statements run).
    pub views: u64,
    /// Overrides that became the tenants' own (`orphan`) rows.
    pub orphaned: u64,
    /// Hide rows removed.
    pub unhidden: u64,
    /// Copy tables dropped.
    pub copies: u64,
}

/// The tenant edges `sh`, and the shares derived from it, carry: read
/// from the parent's state before the drop commits.
#[must_use]
pub fn carried(ctx: &Ctx<'_>, sh: &Share) -> Vec<Carried> {
    ctx.state
        .shares()
        .filter(|s| {
            (s.id == sh.id || s.via == Some(sh.id))
                && s.from == ROOT_SPACE
                && matches!(s.to, ShareTarget::Tenant(_) | ShareTarget::AllChildren)
        })
        .filter_map(|s| {
            let o = ctx.state.object(s.object_id)?;
            Some(Carried {
                object: s.object_id,
                share: s.id,
                db: ctx.db_name(o.db)?,
                table: o.table.clone(),
            })
        })
        .collect()
}

/// The children's side of a committed `DROP SHARE` (module docs).
/// `ctx` is the parent's context from before the drop.
///
/// # Errors
/// The directory's, the cache's and the backends'.
pub async fn after_drop(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    carried: &[Carried],
) -> Result<Unshared> {
    let mut out = Unshared::default();
    if carried.is_empty() {
        return Ok(out);
    }
    let parent = ctx.cat.tenant;
    let fresh = r.tenant_catalog(parent).await?;
    for c in carried {
        let mut tree = r.tree_tables(parent, &c.db, &c.table).await?;
        tree.retain(|t| t.tenant != parent && t.ancestors.contains(&parent));
        // Top first: a child's overrides are its own before its children's
        // are compared with them.
        tree.sort_by_key(|t| t.ancestors.len());
        let at: BTreeMap<TenantId, TreeTable> =
            tree.iter().map(|t| (t.tenant, t.clone())).collect();
        let mut cut: BTreeSet<TenantId> = BTreeSet::new();
        for t in &tree {
            let Some((table, key, object)) = table_of(r, t, c, ctx.lctn).await? else {
                continue;
            };
            if t.ancestors.last() == Some(&parent) {
                // A direct child: did the dropped share serve it?
                let before = ctx.state.tenant_edge(c.object, t.tenant).map(|s| s.id);
                if before != Some(c.share) {
                    continue;
                }
                r.ctl_query(
                    t.tenant,
                    &format!(
                        "DROP VIEW IF EXISTS {}.{}",
                        q(&t.schema),
                        q(&format!("__sprig_share_{}", c.share))
                    ),
                )
                .await?;
                out.views += 1;
                if state_of(&fresh)
                    .and_then(|s| s.tenant_edge(c.object, t.tenant))
                    .is_some()
                {
                    // Another share of the table serves it now.
                    continue;
                }
                cut.insert(t.tenant);
                out.orphaned += orphan(r, t.tenant, &table, &key, None).await?;
                out.unhidden += unhide(r, t.tenant, &table, &key, None).await?;
                out.copies += drop_copy(r, t.tenant, &t.schema, object).await?;
            } else if let Some(i) = t.ancestors.iter().position(|a| cut.contains(a)) {
                // Below a cut child: the levels from that child down.
                let above: Vec<&TreeTable> =
                    t.ancestors[i..].iter().filter_map(|a| at.get(a)).collect();
                let lost = lost_keys(r, t.tenant, &table, &key, &above, c, ctx.lctn, "ovr").await?;
                for part in lost.chunks(CHUNK) {
                    out.orphaned += orphan(r, t.tenant, &table, &key, Some(part)).await?;
                }
                let lost =
                    lost_keys(r, t.tenant, &table, &key, &above, c, ctx.lctn, "hide").await?;
                for part in lost.chunks(CHUNK) {
                    out.unhidden += unhide(r, t.tenant, &table, &key, Some(part)).await?;
                }
            }
        }
    }
    if out.orphaned > 0 {
        metrics::counter!("sprig_inherit_orphans_total", "policy" => "keep", "path" => "unshare")
            .increment(out.orphaned);
    }
    Ok(out)
}

/// Tenant `t`'s inheritable table of `c` (`schema`.`table`), its key and
/// its object.
async fn table_of(
    r: &mut dyn Runner,
    t: &TreeTable,
    c: &Carried,
    lctn: bool,
) -> Result<Option<(String, Vec<String>, ObjectId)>> {
    let cat = r.tenant_catalog(t.tenant).await?;
    let Some(st) = state_of(&cat) else {
        return Ok(None);
    };
    let Some(d) = cat.database(&database_key(&c.db, lctn)) else {
        return Ok(None);
    };
    Ok(st.object_named(d.db_id, &c.table, lctn).map(|o| {
        (
            format!("{}.{}", q(&t.schema), q(&o.table)),
            o.key.clone(),
            o.object_id,
        )
    }))
}

/// Removes tenant `t`'s hide rows (root space): all of them, or those of
/// `keys`. Returns how many went.
async fn unhide(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    keys: Option<&[Vec<String>]>,
) -> Result<u64> {
    let mut cond = format!(
        "`_sprig_space` = {} AND `_sprig_state` = 'hide'",
        ROOT_SPACE.0
    );
    if let Some(k) = keys {
        if k.is_empty() {
            return Ok(0);
        }
        let _ = write!(cond, " AND {}", key_pred(key, k));
    }
    let n = count(r, t, &format!("SELECT COUNT(*) FROM {table} WHERE {cond}")).await?;
    if n > 0 {
        r.ctl_query(t, &format!("DELETE FROM {table} WHERE {cond}"))
            .await?;
    }
    Ok(n)
}

/// The first value of a `COUNT(*)`.
async fn count(r: &mut dyn Runner, t: TenantId, sql: &str) -> Result<u64> {
    Ok(r.ctl_query(t, sql)
        .await?
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .and_then(|b| String::from_utf8_lossy(&b).parse::<u64>().ok())
        .unwrap_or(0))
}

/// Removes the copy of tenant `t`'s `object` from its parent tenant (a
/// materialized edge), with its state row: the whole copy table when no
/// other copy of the object (a share to a space) is left in it. Returns 1
/// when the table went.
async fn drop_copy(r: &mut dyn Runner, t: TenantId, schema: &str, object: ObjectId) -> Result<u64> {
    let copy = copy_table(object);
    let have: BTreeSet<String> = r
        .ctl_query(
            t,
            &format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND \
                 TABLE_NAME IN ({}, {})",
                key_text(Some(schema)),
                key_text(Some(&copy)),
                key_text(Some(STATE_TABLE))
            ),
        )
        .await?
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect();
    if !have.contains(&copy) {
        return Ok(0);
    }
    let copy = format!("{}.{}", q(schema), q(&copy));
    let others = if have.contains(STATE_TABLE) {
        let state = format!("{}.{}", q(schema), q(STATE_TABLE));
        r.ctl_query(
            t,
            &format!(
                "DELETE FROM {state} WHERE object_id = {} AND share_key = {UPSTREAM}",
                object.0
            ),
        )
        .await?;
        count(
            r,
            t,
            &format!(
                "SELECT COUNT(*) FROM {state} WHERE object_id = {}",
                object.0
            ),
        )
        .await?
    } else {
        0
    };
    if others > 0 {
        r.ctl_query(
            t,
            &format!("DELETE FROM {copy} WHERE `_sprig_share` = {UPSTREAM}"),
        )
        .await?;
        return Ok(0);
    }
    r.ctl_query(t, &format!("DROP TABLE IF EXISTS {copy}"))
        .await?;
    Ok(1)
}

/// `(k1, k2) IN ((..), ..)`, or `k IN (..)`.
fn key_pred(key: &[String], keys: &[Vec<String>]) -> String {
    let tuple = |v: &[String]| {
        if v.len() == 1 {
            v[0].clone()
        } else {
            format!("({})", v.join(", "))
        }
    };
    let cols: Vec<String> = key.iter().map(|k| q(k)).collect();
    format!(
        "{} IN ({})",
        tuple(&cols),
        keys.iter().map(|k| tuple(k)).collect::<Vec<_>>().join(", ")
    )
}

/// A result row's values as key literals.
fn key_lits(row: &[Option<Vec<u8>>]) -> Vec<String> {
    row.iter()
        .map(|v| key_text(v.as_deref().map(String::from_utf8_lossy).as_deref()))
        .collect()
}

/// Turns tenant `t`'s overrides (root space) into `orphan` rows: all of
/// them, or those of `keys`. Returns how many there were.
async fn orphan(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    keys: Option<&[Vec<String>]>,
) -> Result<u64> {
    let mut cond = format!(
        "`_sprig_space` = {} AND `_sprig_state` = 'ovr'",
        ROOT_SPACE.0
    );
    if let Some(k) = keys {
        if k.is_empty() {
            return Ok(0);
        }
        let _ = write!(cond, " AND {}", key_pred(key, k));
    }
    let n = r
        .ctl_query(t, &format!("SELECT COUNT(*) FROM {table} WHERE {cond}"))
        .await?
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .and_then(|b| String::from_utf8_lossy(&b).parse::<u64>().ok())
        .unwrap_or(0);
    if n == 0 {
        return Ok(0);
    }
    r.ctl_query(
        t,
        &format!(
            "UPDATE {table} SET `_sprig_state` = 'orphan', `_sprig_ovr` = {ALL_BITS} WHERE {cond}"
        ),
    )
    .await?;
    Ok(n)
}

/// Tenant `t`'s overrides (`state` `ovr`), or hides (`hide`), whose record
/// no level of `above` (top first) still holds, ignoring share filters:
/// the copy worker's orphan test.
#[allow(clippy::too_many_arguments)]
async fn lost_keys(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    above: &[&TreeTable],
    c: &Carried,
    lctn: bool,
    state: &str,
) -> Result<Vec<Vec<String>>> {
    let cols = key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
    let ovr: Vec<Vec<String>> = r
        .ctl_query(
            t,
            &format!(
                "SELECT {cols} FROM {table} WHERE `_sprig_space` = {} AND `_sprig_state` = '{state}'",
                ROOT_SPACE.0
            ),
        )
        .await?
        .iter()
        .map(|row| key_lits(row))
        .collect();
    if ovr.is_empty() {
        return Ok(Vec::new());
    }
    let mut exists: BTreeMap<Vec<String>, bool> = ovr.into_iter().map(|k| (k, false)).collect();
    for y in above {
        let Some((ytable, _, _)) = table_of(r, y, c, lctn).await? else {
            continue;
        };
        let keys: Vec<Vec<String>> = exists.keys().cloned().collect();
        for part in keys.chunks(CHUNK) {
            let rows = r
                .ctl_query(
                    y.tenant,
                    &format!(
                        "SELECT {cols}, CAST(`_sprig_state` AS CHAR) FROM {ytable} WHERE \
                         `_sprig_space` = {} AND {}",
                        ROOT_SPACE.0,
                        key_pred(key, part)
                    ),
                )
                .await?;
            for row in rows {
                let Some((st, k)) = row.split_last() else {
                    continue;
                };
                let st = st
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default();
                if let Some(e) = exists.get_mut(&key_lits(k)) {
                    match st.as_ref() {
                        "hide" => *e = false,
                        "own" | "orphan" => *e = true,
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(exists
        .into_iter()
        .filter(|(_, e)| !e)
        .map(|(k, _)| k)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bugfix-55: a failed cleanup runs again, [`ATTEMPTS`] times in all,
    /// with growing pauses.
    #[test]
    fn cleanup_retries() {
        let mut r = Retry::default();
        assert_eq!(r.again(), Some(Duration::from_millis(200)));
        assert_eq!(r.again(), Some(Duration::from_secs(1)));
        assert_eq!(r.again(), None);
        assert_eq!(r.failed(), ATTEMPTS);
    }

    #[test]
    fn key_predicates() {
        assert_eq!(
            key_pred(&["id".into()], &[vec!["'1'".into()], vec!["'2'".into()]]),
            "`id` IN ('1', '2')"
        );
        assert_eq!(
            key_pred(
                &["a".into(), "b".into()],
                &[vec!["'1'".into(), "'x'".into()]]
            ),
            "(`a`, `b`) IN (('1', 'x'))"
        );
        assert_eq!(
            key_lits(&[Some(b"7".to_vec()), Some(b"o'k".to_vec())]),
            vec!["'7'".to_owned(), "'o''k'".to_owned()]
        );
    }
}
