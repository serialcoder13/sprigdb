//! Decision O1 for user and migrated indexes: the same usage rule as for
//! automatic ones, under `auto_index.retire_user_indexes` (`on` removes,
//! `recommend` only recommends, `off` leaves them alone). State lives in
//! `index_usage` ([`UsageRow`]).
//!
//! ```text
//! object_pass(t), every usage_every while t is read:
//!   probe t on the writer: indexes, foreign keys, SHOW CREATE TABLE
//!   eligible(x) := not PRIMARY, not UNIQUE, not sprig_auto_*, a B-tree, and no
//!                  foreign key of t (or referencing t) has its columns as x's
//!                  leading columns
//!   off: retiring rows NOT IGNORED again, recommended rows serving again; stop
//!   each eligible user or migrated index x:
//!     no row → serving, sample from now (an index the user ignored is skipped)
//!     serving or recommended: check_usage → remove: on → IGNORED, retiring;
//!                                                recommend → recommended
//!                                          keep → serving, next sample from now
//!     retiring: a served fingerprint regressed → NOT IGNORED, serving
//!               usage_sample_reads reads, no regression → DROP INDEX, dropped
//!     pinned: nothing
//!   rows whose index is gone: retiring → dropped (drift); else forgotten
//! ```
//!
//! Every statement here goes through [`user_ddl`], which refuses the
//! primary key and automatic indexes; eligibility is checked again on the
//! probe each pass, so an index that came to back a foreign key is made
//! visible and forgotten.

use std::collections::BTreeMap;
use std::time::SystemTime;

use mysql_async::prelude::Queryable;
use sprig_cluster::sql::literal;
use sprig_common::ids::ObjectId;
use sprig_common::names::{parse_auto_index, quote};
use sprig_common::{Result, SprigError};

use super::usage::UsageVerdict;
use super::{
    IgnoredWatch, ObserveCx, RetireState, UsageState, check_usage, part, run_on_writer,
    watch_ignored, with_part,
};
use crate::autoindex::model::{KeyPart, Origin};
use crate::autoindex::policy::Mode;
use crate::autoindex::rollout::{ShardTable, unix_ms};
use crate::autoindex::store::usage::{UsageRow, UserIndexState as S};
use crate::autoindex::store::{AutoIndexStore, IndexDecision, origin_of};

/// One index of a table, as the probe sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIndex {
    /// The name.
    pub name: String,
    /// Key parts; columns in lower case.
    pub parts: Vec<KeyPart>,
    /// `UNIQUE` (the primary key included).
    pub unique: bool,
    /// `BTREE`, `FULLTEXT`, `SPATIAL`, `HASH`, `VECTOR`.
    pub index_type: String,
    /// `IGNORED`.
    pub ignored: bool,
    /// Its line of `SHOW CREATE TABLE`, without a trailing comma or
    /// `IGNORED`: what restore adds back.
    pub clause: Option<String>,
}

/// A table's indexes and the foreign keys that need some of them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableProbe {
    /// Indexes by name.
    pub indexes: BTreeMap<String, TableIndex>,
    /// Column lists (lower case, in order) of foreign keys of the table and
    /// of foreign keys elsewhere referencing it.
    pub fk_columns: Vec<Vec<String>>,
}

/// Why an index is never removed, `None` when it may be.
#[must_use]
pub fn protected(ix: &TableIndex, fks: &[Vec<String>]) -> Option<&'static str> {
    if ix.name.eq_ignore_ascii_case("PRIMARY") {
        return Some("primary_key");
    }
    if ix.unique {
        return Some("unique");
    }
    if parse_auto_index(&ix.name).is_some() {
        return Some("automatic");
    }
    if !ix.index_type.eq_ignore_ascii_case("BTREE") {
        return Some("not_btree");
    }
    let cols: Vec<&str> = ix.parts.iter().map(|p| p.column.as_str()).collect();
    if fks.iter().any(|fk| {
        !fk.is_empty()
            && fk.len() <= cols.len()
            && fk.iter().zip(&cols).all(|(a, b)| a.eq_ignore_ascii_case(b))
    }) {
        return Some("foreign_key");
    }
    None
}

/// The clause of index `name` in a `SHOW CREATE TABLE` text: its
/// `KEY ...` line without the trailing comma and without `IGNORED`.
#[must_use]
pub fn clause_of(create: &str, name: &str) -> Option<String> {
    let head = format!("KEY {} ", quote(name));
    let head_paren = format!("KEY {}(", quote(name));
    create.lines().find_map(|l| {
        let t = l.trim().trim_end_matches(',');
        if !(t.starts_with(&head) || t.starts_with(&head_paren)) {
            return None;
        }
        let t = t.strip_suffix(" IGNORED").unwrap_or(t);
        Some(t.to_owned())
    })
}

fn sv(r: &mysql_async::Row, i: usize) -> Option<String> {
    match r.as_ref(i) {
        Some(mysql_async::Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        Some(mysql_async::Value::Int(n)) => Some(n.to_string()),
        Some(mysql_async::Value::UInt(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// Probes `t` on `ep`: `None` when the table doesn't exist.
#[allow(clippy::many_single_char_names)]
///
/// # Errors
/// The node is unreachable or a query failed.
pub async fn probe(
    ep: &sprig_cluster::sql::NodeEndpoint,
    t: &ShardTable,
) -> std::result::Result<Option<TableProbe>, String> {
    let mut c = mysql_async::Conn::new(ep.opts.clone())
        .await
        .map_err(|e| e.to_string())?;
    let r = async {
        let (s, n) = (literal(&t.schema), literal(&t.table));
        let rows: Vec<mysql_async::Row> = c
            .query(format!(
                "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART, COLLATION, NON_UNIQUE, IGNORED, \
                 INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {s} \
                 AND TABLE_NAME = {n} ORDER BY INDEX_NAME, SEQ_IN_INDEX"
            ))
            .await
            .map_err(|e| e.to_string())?;
        let exists: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = {s} \
                 AND TABLE_NAME = {n}"
            ))
            .await
            .map_err(|e| e.to_string())?;
        if exists != Some(1) {
            return Ok(None);
        }
        let mut p = TableProbe::default();
        for r in &rows {
            let Some(name) = sv(r, 0) else { continue };
            let e = p
                .indexes
                .entry(name.clone())
                .or_insert_with(|| TableIndex {
                    name,
                    parts: Vec::new(),
                    unique: sv(r, 4).as_deref() == Some("0"),
                    index_type: sv(r, 6).unwrap_or_default(),
                    ignored: sv(r, 5).is_some_and(|v| v.eq_ignore_ascii_case("YES")),
                    clause: None,
                });
            e.parts.push(KeyPart {
                column: sv(r, 1).unwrap_or_default().to_ascii_lowercase(),
                prefix: sv(r, 2).and_then(|v| v.parse().ok()),
                desc: sv(r, 3).as_deref() == Some("D"),
            });
        }
        let fks: Vec<mysql_async::Row> = c
            .query(format!(
                "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, \
                 REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ((TABLE_SCHEMA = {s} AND \
                 TABLE_NAME = {n}) OR (REFERENCED_TABLE_SCHEMA = {s} AND REFERENCED_TABLE_NAME = {n})) \
                 ORDER BY CONSTRAINT_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"
            ))
            .await
            .map_err(|e| e.to_string())?;
        // Per constraint: the child's columns when the table is the
        // child, the referenced columns when it is the parent (both, for a
        // table referencing itself).
        let mut by: BTreeMap<(String, String, String, bool), Vec<String>> = BTreeMap::new();
        let here = |a: Option<String>, b: Option<String>| {
            a.as_deref() == Some(t.schema.as_str()) && b.as_deref() == Some(t.table.as_str())
        };
        for r in &fks {
            let key = |child: bool| {
                (
                    sv(r, 0).unwrap_or_default(),
                    sv(r, 3).unwrap_or_default(),
                    sv(r, 1).unwrap_or_default(),
                    child,
                )
            };
            if here(sv(r, 2), sv(r, 3)) {
                by.entry(key(true))
                    .or_default()
                    .push(sv(r, 4).unwrap_or_default().to_ascii_lowercase());
            }
            if here(sv(r, 6), sv(r, 7)) {
                by.entry(key(false))
                    .or_default()
                    .push(sv(r, 5).unwrap_or_default().to_ascii_lowercase());
            }
        }
        p.fk_columns = by.into_values().collect();
        let create: Option<(String, String)> = c
            .query_first(format!(
                "SHOW CREATE TABLE {}.{}",
                quote(&t.schema),
                quote(&t.table)
            ))
            .await
            .map_err(|e| e.to_string())?;
        if let Some((_, text)) = create {
            for ix in p.indexes.values_mut() {
                ix.clause = clause_of(&text, &ix.name);
            }
        }
        Ok(Some(p))
    }
    .await;
    let _ = c.disconnect().await;
    r
}

/// An index statement on a user or migrated index: `ALTER TABLE s.t
/// <clause>`. Refuses the primary key and automatic indexes.
///
/// # Errors
/// `Internal` for a refused name.
pub fn user_ddl(t: &ShardTable, name: &str, clause: &str) -> Result<String> {
    if parse_auto_index(name).is_some() || name.eq_ignore_ascii_case("PRIMARY") {
        return Err(SprigError::internal(format!(
            "refusing user-index DDL on {name}"
        )));
    }
    Ok(format!(
        "ALTER TABLE {}.{} {clause}",
        quote(&t.schema),
        quote(&t.table)
    ))
}

/// `ALTER INDEX x [NOT] IGNORED`.
///
/// # Errors
/// As [`user_ddl`].
pub fn ignore_user_sql(t: &ShardTable, name: &str, ignored: bool) -> Result<String> {
    user_ddl(
        t,
        name,
        &format!(
            "ALTER INDEX {} {}",
            quote(name),
            if ignored { "IGNORED" } else { "NOT IGNORED" }
        ),
    )
}

/// `DROP INDEX IF EXISTS x`.
///
/// # Errors
/// As [`user_ddl`].
pub fn drop_user_sql(t: &ShardTable, name: &str) -> Result<String> {
    user_ddl(t, name, &format!("DROP INDEX IF EXISTS {}", quote(name)))
}

/// `ADD <clause>`: recreates a dropped index from its saved definition.
///
/// # Errors
/// As [`user_ddl`]; `Internal` for a clause that isn't one plain `KEY`.
pub fn add_user_sql(t: &ShardTable, name: &str, clause: &str) -> Result<String> {
    if !clause.starts_with(&format!("KEY {}", quote(name))) || clause.contains(';') {
        return Err(SprigError::internal(format!(
            "refusing to restore {name} from {clause:?}"
        )));
    }
    user_ddl(t, name, &format!("ADD {clause}"))
}

/// What one user index's step did.
#[derive(Debug, Clone, PartialEq)]
pub enum UserStep {
    /// First seen: judged from now on.
    Tracked,
    /// Reads counted, or judged and kept.
    Checked,
    /// Judged removable; recommended only.
    Recommended,
    /// Judged removable: `IGNORED`, watched.
    Retiring,
    /// Visible again: why.
    Restored(String),
    /// Dropped.
    Dropped,
    /// Its row was forgotten (the user dropped it, or it became protected).
    Forgotten,
    /// A statement failed; tried again next pass.
    Failed(String),
}

fn new_row(object: ObjectId, ix: &TableIndex, origin: Origin, now: SystemTime) -> UsageRow {
    let us = UsageState {
        since_ms: unix_ms(now),
        ..UsageState::default()
    };
    UsageRow {
        object,
        index_name: ix.name.clone(),
        origin,
        state: S::Serving,
        definition: ix.clause.clone().unwrap_or_default(),
        observed: with_part(None, "usage", &us),
        drop_reason: None,
        created_at: now,
        state_changed_at: now,
    }
}

async fn on_all_shards(
    cx: &ObserveCx<'_>,
    sql: impl Fn(&ShardTable) -> Result<String>,
) -> std::result::Result<(), String> {
    for t in cx.shards {
        let s = sql(t).map_err(|e| e.to_string())?;
        run_on_writer(cx, t, &s).await?;
    }
    Ok(())
}

async fn log(
    cx: &ObserveCx<'_>,
    r: &UsageRow,
    kind: &'static str,
    mode: &'static str,
    reason: &str,
    summary: String,
    evidence: serde_json::Value,
) {
    let d = IndexDecision {
        tenant: cx.tenant,
        kind,
        subject_type: "index",
        subject_id: format!("{}/{}", r.object.0, r.index_name),
        mode,
        outcome: "succeeded",
        reason_code: reason.to_owned(),
        summary,
        evidence,
        alternatives: None,
        at: cx.now,
    };
    if let Err(e) = cx.decisions.record(&d).await {
        tracing::warn!(error = %e, "decision not recorded");
    }
}

/// Makes a retiring index visible again and its row `serving`, a new
/// sample starting now.
async fn bring_back<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    r: &UsageRow,
    from: S,
    why: &str,
) -> Result<UserStep> {
    if from == S::Retiring {
        let name = r.index_name.clone();
        if let Err(e) = on_all_shards(cx, |t| ignore_user_sql(t, &name, false)).await {
            return Ok(UserStep::Failed(e));
        }
    }
    let mut us: UsageState = part(Some(&r.observed), "usage");
    us.since_ms = unix_ms(cx.now);
    us.reads = 0;
    store
        .set_usage_observed(
            q,
            r.object,
            &r.index_name,
            &with_part(Some(&r.observed), "usage", &us),
        )
        .await?;
    if store
        .set_usage_state(q, r.object, &r.index_name, from, S::Serving, None, cx.now)
        .await?
    {
        log(
            cx,
            r,
            "index.restore",
            "applied",
            why,
            format!("{} visible again and kept ({why})", r.index_name),
            serde_json::json!({"from": from.as_str()}),
        )
        .await;
    }
    Ok(UserStep::Restored(why.to_owned()))
}

/// One pass over the user and migrated indexes of `object` (module docs).
///
/// # Errors
/// Catalog errors; statement failures are [`UserStep::Failed`].
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the state machine, in one place
pub async fn object_pass<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    object: ObjectId,
) -> Result<Vec<(String, UserStep)>> {
    let mut out = Vec::new();
    let Some(first) = cx.shards.first() else {
        return Ok(out);
    };
    let rows: BTreeMap<String, UsageRow> = store
        .usage_rows(q, Some(object))
        .await?
        .into_iter()
        .map(|r| (r.index_name.clone(), r))
        .collect();
    let mode = cx.rules.retire_user_indexes;
    let Some(w) = cx.view.nodes.writer() else {
        return Ok(vec![(String::new(), UserStep::Failed("no writer".into()))]);
    };
    let probe = match probe(w, first).await {
        Ok(Some(p)) => p,
        Ok(None) => return Ok(out),
        Err(e) => return Ok(vec![(String::new(), UserStep::Failed(e))]),
    };
    if mode == Mode::Off {
        for r in rows.values() {
            if matches!(r.state, S::Retiring | S::Recommended) {
                let st = bring_back(store, q, cx, r, r.state, "retire_user_indexes_off").await?;
                out.push((r.index_name.clone(), st));
            }
        }
        return Ok(out);
    }
    let recorded = store.origins(q, object).await?;
    let now_ms = unix_ms(cx.now);
    let due = |r: &UsageRow| {
        let us: UsageState = part(Some(&r.observed), "usage");
        now_ms.saturating_sub(us.last_check_ms) >= super::ms(cx.settings.usage_every)
    };
    for ix in probe.indexes.values() {
        let origin = origin_of(&ix.name, recorded.get(&ix.name).copied());
        if !matches!(origin, Origin::User | Origin::Migrated) {
            continue;
        }
        let row = rows.get(&ix.name);
        if let Some(why) = protected(ix, &probe.fk_columns) {
            if let Some(r) = row {
                if r.state == S::Retiring {
                    let st = bring_back(store, q, cx, r, S::Retiring, why).await?;
                    out.push((ix.name.clone(), st));
                }
                store.delete_usage(q, object, &ix.name).await?;
                out.push((ix.name.clone(), UserStep::Forgotten));
            }
            continue;
        }
        let Some(r) = row.filter(|r| r.state != S::Dropped) else {
            // A user's own IGNORED index is theirs to decide about.
            if !ix.ignored && ix.clause.is_some() {
                store
                    .put_usage(q, &new_row(object, ix, origin, cx.now))
                    .await?;
                out.push((ix.name.clone(), UserStep::Tracked));
            }
            continue;
        };
        match r.state {
            S::Pinned | S::Dropped => {}
            S::Serving | S::Recommended => {
                if !due(r) {
                    continue;
                }
                let mut us: UsageState = part(Some(&r.observed), "usage");
                let columns: Vec<String> = ix.parts.iter().map(|p| p.column.clone()).collect();
                let judged =
                    match check_usage(cx, object, &ix.name, &columns, None, &mut us, r.created_at)
                        .await
                    {
                        Ok(j) => j,
                        Err(e) => {
                            out.push((ix.name.clone(), UserStep::Failed(e)));
                            continue;
                        }
                    };
                let mut step = UserStep::Checked;
                let mut observed_after = None;
                // Chapter 52: `on` and `approve` ask the gate first; it holds
                // the removal under approve (the row shows as recommended).
                let gate = match &judged {
                    Some(j) if j.verdict == UsageVerdict::Remove => {
                        if matches!(mode, Mode::On | Mode::Approve)
                            && (mode == Mode::On || cx.removals.is_some())
                        {
                            let proposal = super::Removal {
                                object,
                                index: &ix.name,
                                user: true,
                                reason: "unused",
                                columns: ix.parts.iter().map(|p| p.column.clone()).collect(),
                                evidence: serde_json::json!({"usage": us.last, "origin": origin.as_str()}),
                            };
                            Some(super::gate_removal(cx, &proposal).await)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                let mut ticket: Option<String> = None;
                if let Some(j) = &judged {
                    match (j.verdict, &gate) {
                        (UsageVerdict::NotYet, _) => {}
                        (UsageVerdict::Remove, Some(super::RemovalVerdict::Go(t))) => {
                            ticket.clone_from(t);
                            if let Err(e) =
                                on_all_shards(cx, |t| ignore_user_sql(t, &ix.name, true)).await
                            {
                                super::removal_done(cx, ticket.as_deref(), false, &e).await;
                                out.push((ix.name.clone(), UserStep::Failed(e)));
                                continue;
                            }
                            us.verdict = Some("remove".into());
                            let rs = RetireState {
                                since_ms: now_ms,
                                p95_before_ms: None,
                                before: j.before(),
                                reads: 0,
                            };
                            let o = with_part(Some(&r.observed), "usage", &us);
                            observed_after = Some(with_part(Some(&o), "retire", &rs));
                            step = UserStep::Retiring;
                        }
                        (UsageVerdict::Remove, Some(super::RemovalVerdict::Hold(_))) => {
                            // Held for approval: recommended, and judged
                            // again on the same sample when due, so the item
                            // stays fresh while the evidence holds.
                            us.verdict = Some("remove".into());
                            step = UserStep::Recommended;
                        }
                        (UsageVerdict::Remove, _) => {
                            // recommend: recorded, judged again on the next sample.
                            us.verdict = Some("remove".into());
                            j.next_sample(&mut us, now_ms);
                            step = UserStep::Recommended;
                        }
                        (v @ (UsageVerdict::Keep | UsageVerdict::Unknown), _) => {
                            us.verdict = Some(
                                if v == UsageVerdict::Keep {
                                    "keep"
                                } else {
                                    "unknown"
                                }
                                .into(),
                            );
                            j.next_sample(&mut us, now_ms);
                        }
                    }
                }
                let observed =
                    observed_after.unwrap_or_else(|| with_part(Some(&r.observed), "usage", &us));
                store
                    .set_usage_observed(q, object, &ix.name, &observed)
                    .await?;
                let evidence = serde_json::json!({"usage": us.last, "settings": {
                    "auto_index.usage_sample_reads": cx.rules.usage_sample_reads,
                    "auto_index.min_usage_ratio": cx.rules.min_usage_ratio,
                    "auto_index.retire_user_indexes": match mode { Mode::On => "on", Mode::Recommend => "recommend", Mode::Approve => "approve", Mode::Off => "off" },
                }});
                match (&step, r.state) {
                    (UserStep::Retiring, from) => {
                        if store
                            .set_usage_state(
                                q,
                                object,
                                &ix.name,
                                from,
                                S::Retiring,
                                Some("unused"),
                                cx.now,
                            )
                            .await?
                        {
                            log(cx, r, "index.retire", "applied", "unused", format!(
                                "{} ({}) ignored: rarely used and costing more than it saves; dropped after {} more reads unless a query it served gets slower",
                                ix.name, origin.as_str(), cx.rules.usage_sample_reads), evidence).await;
                            super::removal_done(cx, ticket.as_deref(), true, &format!(
                                "{} ignored; dropped after {} more reads unless a query it served gets slower",
                                ix.name, cx.rules.usage_sample_reads)).await;
                        }
                    }
                    (UserStep::Recommended, S::Serving) => {
                        if store
                            .set_usage_state(
                                q,
                                object,
                                &ix.name,
                                S::Serving,
                                S::Recommended,
                                Some("unused"),
                                cx.now,
                            )
                            .await?
                        {
                            log(cx, r, "index.retire", "recommended", "unused", format!(
                                "{} ({}) is rarely used and costs more than it saves: recommended for removal ({})",
                                ix.name, origin.as_str(), match &gate {
                                    Some(super::RemovalVerdict::Hold(why)) => why.clone(),
                                    _ => "auto_index.retire_user_indexes = recommend".to_owned(),
                                }), evidence).await;
                        }
                    }
                    (UserStep::Checked, S::Recommended)
                        if judged
                            .as_ref()
                            .is_some_and(|j| j.verdict == UsageVerdict::Keep) =>
                    {
                        if store
                            .set_usage_state(
                                q,
                                object,
                                &ix.name,
                                S::Recommended,
                                S::Serving,
                                None,
                                cx.now,
                            )
                            .await?
                        {
                            log(
                                cx,
                                r,
                                "index.keep",
                                "recommended",
                                "used",
                                format!(
                                    "{} is used again: no longer recommended for removal",
                                    ix.name
                                ),
                                evidence,
                            )
                            .await;
                        }
                    }
                    _ => {}
                }
                out.push((ix.name.clone(), step));
            }
            S::Retiring => {
                if !ix.ignored {
                    // Someone made it visible again: theirs to keep.
                    let st = bring_back(store, q, cx, r, S::Retiring, "made_visible").await?;
                    out.push((ix.name.clone(), st));
                    continue;
                }
                let mut rs: RetireState = part(Some(&r.observed), "retire");
                match watch_ignored(cx, object, &rs, &[]).await? {
                    IgnoredWatch::Wait(reads) => {
                        if reads != rs.reads {
                            rs.reads = reads;
                            store
                                .set_usage_observed(
                                    q,
                                    object,
                                    &ix.name,
                                    &with_part(Some(&r.observed), "retire", &rs),
                                )
                                .await?;
                        }
                    }
                    IgnoredWatch::Regressed(g) => {
                        let why = format!("regressed_while_ignored:{}", g.fp);
                        let st = bring_back(store, q, cx, r, S::Retiring, &why).await?;
                        out.push((ix.name.clone(), st));
                    }
                    IgnoredWatch::Drop(reads) => {
                        // Checked again just before the drop: still eligible.
                        if let Err(e) = on_all_shards(cx, |t| drop_user_sql(t, &ix.name)).await {
                            out.push((ix.name.clone(), UserStep::Failed(e)));
                            continue;
                        }
                        rs.reads = reads;
                        store
                            .set_usage_observed(
                                q,
                                object,
                                &ix.name,
                                &with_part(Some(&r.observed), "retire", &rs),
                            )
                            .await?;
                        if store
                            .set_usage_state(
                                q,
                                object,
                                &ix.name,
                                S::Retiring,
                                S::Dropped,
                                Some("unused"),
                                cx.now,
                            )
                            .await?
                        {
                            metrics::counter!("sprig_advisor_autoindex_drops_total", "reason" => "unused_user").increment(1);
                            log(cx, r, "index.drop", "applied", "unused", format!(
                                "{} ({}) dropped after {reads} reads with it ignored and no query slower; `sprigctl index restore` recreates it from: {}",
                                ix.name, origin.as_str(), r.definition), serde_json::json!({"definition": r.definition})).await;
                        }
                        out.push((ix.name.clone(), UserStep::Dropped));
                    }
                }
            }
        }
    }
    // Rows whose index is gone.
    for r in rows.values() {
        if probe.indexes.contains_key(&r.index_name) {
            continue;
        }
        match r.state {
            S::Dropped => {}
            S::Retiring => {
                if store
                    .set_usage_state(
                        q,
                        object,
                        &r.index_name,
                        S::Retiring,
                        S::Dropped,
                        Some("drift"),
                        cx.now,
                    )
                    .await?
                {
                    out.push((r.index_name.clone(), UserStep::Dropped));
                }
            }
            _ => {
                store.delete_usage(q, object, &r.index_name).await?;
                out.push((r.index_name.clone(), UserStep::Forgotten));
            }
        }
    }
    Ok(out)
}
