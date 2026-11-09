//! Applying the first design (chapter 16, "Applying the first design
//! (R1)"; build step 9). While a group's data is still on one shard the
//! plan copies nothing:
//!
//! 1. Write the `table_groups` rows (`preparing`, origin `advisor`, or
//!    `pinned` when the root is pinned `SHARD KEY`) and members
//!    (`rk_state = absent`, `pinned = 1` when a pin placed them), with group ids from the catalog's
//!    `sprig.next_group_id`; write each group's shard map with one shard
//!    (shard 1, the tenant's cluster) and create its schemas.
//! 2. **Regroup.** Under a hold of the tenant's map (at most
//!    `mover.cutover_max_hold_ms`), move every member into its group's
//!    schema with one atomic `RENAME TABLE` on the same server (triggers
//!    are dropped before and re-created after, inside the hold), then
//!    release.
//! 3. Add `_sprig_rk` (an online `ALTER`), mark members `filling` (from now
//!    on writers fill it), and back-fill it: column members by primary-key
//!    chunks hashed in Rust, parent members by copying the co-located
//!    parent's hash, parents before children. Members become `ready` when
//!    no `NULL` is left and sampled rows re-hash correctly.
//! 4. Mark the groups `active`: chapter 20 may split them.
//!
//! Every step checks what is already done, so a new leader resumes a run.
//! Unique lookup indexes (step 4 of the book) are chapter 19's and not
//! built here.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};
use serde_json::json;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::pins::PinKind;
use sprig_shard::rk::{RK_COLUMN, add_rk_column, backfill_columns_sql, backfill_parent_sql};
use sprig_shard::{KeySpec, ShardMap, ShardMapStore, TenantMaps, shard_schema};

use super::design::{Design, Input, Obj, Routing};
use super::input::Located;

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

/// The routing hash of key values read from MariaDB under `spec`
/// (`sprig_shard::rk::hash_values`, which the mover's re-key shares).
///
/// # Errors
/// 39400 for a value the class can't take; 39405 for one that can't be
/// normalized.
pub fn rk_of(spec: &KeySpec, vals: &[Value]) -> Result<u64> {
    sprig_shard::rk::hash_values(spec, vals)
}

/// Publishes a change the apply made to the tenant's catalog rows
/// (`table_groups`, `group_members`) as a new catalog version, so
/// gateways reload their snapshot and route the moved tables (chapter 17's
/// layout reads them). The controller commits an empty versioned change.
#[async_trait::async_trait]
pub trait Publish: Send + Sync + std::fmt::Debug {
    /// Raises `tenant`'s catalog version.
    ///
    /// # Errors
    /// The catalog's.
    async fn catalog_changed(&self, tenant: TenantId) -> Result<()>;
}

/// How long the regroup's hold waits after publishing, so gateways load
/// the new snapshot before writes resume.
const PUBLISH_SETTLE: Duration = Duration::from_millis(300);

/// Where the apply happens.
#[derive(Debug, Clone)]
pub struct ApplyCtx {
    /// The tenant.
    pub tenant: TenantId,
    /// Its cluster (where its schemas are).
    pub cluster: ClusterId,
    /// `mover.cutover_max_hold_ms`.
    pub max_hold: Duration,
    /// `mover.txn_grace_ms`: open transactions drain this long.
    pub txn_grace: Duration,
    /// `mover.hold_grace_ms`.
    pub hold_grace: Duration,
    /// Rows per back-fill chunk (`mover.copy_chunk_rows`).
    pub chunk_rows: usize,
    /// Settle time after marking members `filling` before the back-fill,
    /// so writers have seen it.
    pub fill_settle: Duration,
    /// Publishes catalog changes to gateways; `None` (tests that route by
    /// themselves) publishes nothing.
    pub publish: Option<std::sync::Arc<dyn Publish>>,
}

/// One group as created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedGroup {
    /// Its id.
    pub group: GroupId,
    /// Its name.
    pub name: String,
    /// Its key.
    pub key: KeySpec,
    /// Members: object, database, table name, routing.
    pub members: Vec<(Obj, DbId, String, Routing)>,
}

/// What an apply did.
#[derive(Debug, Clone, PartialEq)]
pub struct Applied {
    /// The groups.
    pub groups: Vec<CreatedGroup>,
    /// The regroup's write hold, ms.
    pub hold_ms: u64,
    /// Rows back-filled.
    pub backfilled: u64,
    /// The plan, as `advisor_runs.plan` records it.
    pub plan: serde_json::Value,
}

fn catalog(t: TenantId) -> String {
    SchemaName::catalog(t).to_string()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Step 1: the groups this run created before, or new ones.
async fn create_groups(
    c: &mut Conn,
    ctx: &ApplyCtx,
    run: u64,
    design: &Design,
    input: &Input,
    located: &[Located],
) -> Result<Vec<CreatedGroup>> {
    let cat = catalog(ctx.tenant);
    sprig_shard::groups::migrate(c, &cat).await?;
    let existing: Vec<(u32, String)> = c
        .exec(
            format!("SELECT group_id, name FROM `{cat}`.table_groups WHERE advisor_run_id = ? ORDER BY group_id"),
            (run,),
        )
        .await
        .map_err(|e| db_err("table groups", &e))?;
    let first = if existing.is_empty() {
        // `sprig.next_group_id`, kept past every group id in use.
        c.exec_drop(
            format!(
                "INSERT INTO `{cat}`.settings (scope_kind, scope_id, setting_key, value, bounds, \
                 set_by, set_at, version) VALUES ('tenant', 0, 'sprig.next_group_id', '1', NULL, \
                 'system', UTC_TIMESTAMP(6), 0) ON DUPLICATE KEY UPDATE setting_key = setting_key"
            ),
            (),
        )
        .await
        .map_err(|e| db_err("group counter", &e))?;
        let floor: Option<u64> = c
            .query_first(format!(
                "SELECT GREATEST(COALESCE((SELECT MAX(default_group_id) FROM `{cat}`.`databases`), 0), \
                 COALESCE((SELECT MAX(group_id) FROM `{cat}`.table_groups), 0)) + 1"
            ))
            .await
            .map_err(|e| db_err("group ids", &e))?;
        let n = u64::try_from(design.groups.len()).unwrap_or(0);
        c.exec_drop(
            format!(
                "UPDATE `{cat}`.settings SET value = LAST_INSERT_ID(GREATEST(CAST(value AS UNSIGNED), ?) + ?), \
                 set_at = UTC_TIMESTAMP(6) WHERE scope_kind = 'tenant' AND scope_id = 0 \
                 AND setting_key = 'sprig.next_group_id'"
            ),
            (floor.unwrap_or(1), n),
        )
        .await
        .map_err(|e| db_err("group ids", &e))?;
        let next: Option<u64> = c
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .map_err(|e| db_err("group ids", &e))?;
        Some(next.unwrap_or(n) - n)
    } else {
        None
    };
    let mut out = Vec::new();
    for (i, g) in design.groups.iter().enumerate() {
        let id = match first {
            Some(f) => GroupId(
                u32::try_from(f + i as u64)
                    .map_err(|_| SprigError::internal("group ids exhausted"))?,
            ),
            None => GroupId(
                existing
                    .iter()
                    .find(|(_, n)| n == &g.name)
                    .map(|(id, _)| *id)
                    .ok_or_else(|| {
                        SprigError::internal(format!("run {run} lost group {}", g.name))
                    })?,
            ),
        };
        let key = g.key_spec()?;
        let mut members = Vec::new();
        for m in &g.members {
            let l = located
                .iter()
                .find(|l| l.object == m.object)
                .ok_or_else(|| {
                    SprigError::internal(format!("object {} has no schema", m.object))
                })?;
            members.push((m.object, l.db, l.name.clone(), m.routing.clone()));
        }
        if first.is_some() {
            // Chapter 15: a group whose root is pinned SHARD KEY is
            // `pinned`; members placed by a pin have `pinned = 1`.
            let origin = match g.members.first().and_then(|m| input.pin(m.object)) {
                Some(PinKind::ShardKey { .. }) => "pinned",
                _ => "advisor",
            };
            c.exec_drop(
                format!(
                    "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, \
                     state, origin, advisor_run_id, created_at, updated_at) VALUES \
                     (?, 'sharded', ?, ?, ?, 'preparing', ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
                ),
                (id.0, &g.name, key.to_json().to_string(), key.digest, origin, run),
            )
            .await
            .map_err(|e| db_err("insert a group", &e))?;
            for (o, db, _, r) in &members {
                let (routing, cols, parent, fk) = match r {
                    Routing::Columns { columns } => (
                        "columns",
                        Some(serde_json::to_string(columns).unwrap_or_default()),
                        None,
                        None,
                    ),
                    Routing::Parent { parent, fk } => {
                        ("parent", None, Some(*parent), Some(fk.clone()))
                    }
                };
                let pinned = matches!(
                    input.pin(*o),
                    Some(PinKind::ShardKey { .. } | PinKind::Colocate { .. })
                );
                c.exec_drop(
                    format!(
                        "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, \
                         key_columns, parent_object_id, parent_fk, rk_state, pinned, joined_at) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, 'absent', ?, UTC_TIMESTAMP(6)) \
                         ON DUPLICATE KEY UPDATE object_id = object_id"
                    ),
                    (o, id.0, db.0, routing, cols, parent, fk, pinned),
                )
                .await
                .map_err(|e| db_err("insert a member", &e))?;
            }
        }
        out.push(CreatedGroup {
            group: id,
            name: g.name.clone(),
            key,
            members,
        });
    }
    Ok(out)
}

/// The triggers of `schema.table`, as `CREATE TRIGGER` statements.
async fn triggers(c: &mut Conn, schema: &str, table: &str) -> Result<Vec<(String, String)>> {
    let names: Vec<String> = c
        .exec(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ? \
             AND EVENT_OBJECT_TABLE = ?",
            (schema, table),
        )
        .await
        .map_err(|e| db_err("triggers", &e))?;
    let mut out = Vec::new();
    for n in names {
        let r: Option<Row> = c
            .query_first(format!(
                "SHOW CREATE TRIGGER {}.{}",
                quote(schema),
                quote(&n)
            ))
            .await
            .map_err(|e| db_err("SHOW CREATE TRIGGER", &e))?;
        if let Some(body) = r.and_then(|r| r.get::<String, _>(2)) {
            out.push((n, body));
        }
    }
    Ok(out)
}

/// Step 2: the regroup under the tenant map's hold. Returns the hold, ms.
async fn regroup(
    c: &mut Conn,
    ctx: &ApplyCtx,
    run: u64,
    groups: &[CreatedGroup],
    located: &[Located],
    maps: &TenantMaps,
    shard_maps: &ShardMapStore,
) -> Result<u64> {
    // What is still to move.
    let mut moves: Vec<(String, String, String)> = Vec::new(); // (from, to, table)
    for g in groups {
        for (o, db, name, _) in &g.members {
            let from = located
                .iter()
                .find(|l| l.object == *o)
                .map(|l| l.schema.clone())
                .unwrap_or_default();
            let to = shard_schema(ctx.tenant, *db, g.group, ShardId(1));
            let there: Option<u64> = c
                .exec_first(
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    (&to, name),
                )
                .await
                .map_err(|e| db_err("look up a table", &e))?;
            if there.unwrap_or(0) == 0 {
                moves.push((from, to, name.clone()));
            }
        }
    }
    // Schemas and maps first: writers retrying after the rename find them.
    for g in groups {
        let dbs: std::collections::BTreeSet<DbId> = g.members.iter().map(|m| m.1).collect();
        for db in dbs {
            let to = shard_schema(ctx.tenant, db, g.group, ShardId(1));
            let from = located
                .iter()
                .find(|l| l.db == db)
                .map(|l| l.schema.clone())
                .unwrap_or_default();
            let create: Option<(String, String)> = c
                .query_first(format!("SHOW CREATE DATABASE {}", quote(&from)))
                .await
                .map_err(|e| db_err("SHOW CREATE DATABASE", &e))?;
            let sql = create.map_or_else(
                || format!("CREATE DATABASE IF NOT EXISTS {}", quote(&to)),
                |(_, s)| {
                    s.replacen("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
                        .replace(&quote(&from), &quote(&to))
                },
            );
            c.query_drop(sql)
                .await
                .map_err(|e| db_err("create a group schema", &e))?;
            // Chapter 18: a shard schema anchors transaction decisions;
            // splits and moves copy the table into every child.
            c.query_drop(sprig_catalog::ddl::dtx_table_sql(&to))
                .await
                .map_err(|e| db_err("create the decision table", &e))?;
        }
        if shard_maps.load(ctx.tenant, g.group).await.is_err() {
            let m = ShardMap::presplit(
                ctx.tenant,
                g.group,
                g.key.digest,
                1,
                std::slice::from_ref(&ctx.cluster),
            )?;
            match shard_maps.create(&m).await {
                Ok(_) => {}
                Err(e) if e.code() == Some(SprigCode::CatalogConflict) => {}
                Err(e) => return Err(e),
            }
        }
    }
    if moves.is_empty() {
        return Ok(0);
    }
    let mut trig: Vec<(String, String, String)> = Vec::new(); // (to schema, name, body)
    for (from, to, t) in &moves {
        for (n, body) in triggers(c, from, t).await? {
            trig.push((to.clone(), n, body));
        }
    }
    let holder = format!("advisor-run-{run}");
    let until = now_ms() + u64::try_from((ctx.max_hold + ctx.hold_grace).as_millis()).unwrap_or(0);
    let t0 = Instant::now();
    maps.update(ctx.tenant, |m| {
        sprig_shard::map::hold(m, &holder, until)?;
        Ok(true)
    })
    .await?;
    let release = |maps: &TenantMaps| {
        let maps = maps.clone();
        let holder = holder.clone();
        async move {
            maps.update(ctx.tenant, |m| Ok(sprig_shard::map::release(m, &holder)))
                .await
        }
    };
    let budget = ctx.max_hold.saturating_sub(ctx.txn_grace);
    let attempt = async {
        tokio::time::sleep(ctx.txn_grace).await;
        for (from, _, t) in &moves {
            for (n, _) in triggers(c, from, t).await? {
                c.query_drop(format!(
                    "DROP TRIGGER IF EXISTS {}.{}",
                    quote(from),
                    quote(&n)
                ))
                .await
                .map_err(|e| db_err("drop a trigger", &e))?;
            }
        }
        let wait = budget.as_secs().max(1);
        let pairs = moves
            .iter()
            .map(|(f, to, t)| format!("{}.{} TO {}.{}", quote(f), quote(t), quote(to), quote(t)))
            .collect::<Vec<_>>()
            .join(", ");
        c.query_drop(format!(
            "SET STATEMENT lock_wait_timeout = {wait} FOR RENAME TABLE {pairs}"
        ))
        .await
        .map_err(|e| db_err("regroup rename", &e))?;
        for (to, _, body) in &trig {
            c.query_drop(format!("USE {}", quote(to)))
                .await
                .map_err(|e| db_err("use", &e))?;
            c.query_drop(body.as_str())
                .await
                .map_err(|e| db_err("re-create a trigger", &e))?;
        }
        // Gateways route the moved tables from the next catalog version:
        // publish it while writes are still held.
        if let Some(p) = &ctx.publish {
            p.catalog_changed(ctx.tenant).await?;
            tokio::time::sleep(PUBLISH_SETTLE).await;
        }
        Ok::<(), SprigError>(())
    };
    let r = tokio::time::timeout(ctx.max_hold + ctx.hold_grace, attempt).await;
    let released = release(maps).await;
    let held = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(SprigError::new(
                SprigCode::MoveCutoverAborted,
                "the regroup ran over mover.cutover_max_hold_ms",
            ));
        }
    }
    released?;
    tracing::info!(tenant = %ctx.tenant, tables = moves.len(), held_ms = held, "regrouped");
    Ok(held)
}

/// One chunk of a column member's back-fill; returns the rows written and
/// the last key read.
async fn backfill_columns_chunk(
    c: &mut Conn,
    schema: &str,
    table: &str,
    pk: &str,
    cols: &[String],
    key: &KeySpec,
    after: Option<Value>,
    n: usize,
) -> Result<(u64, Option<Value>)> {
    let list = cols.iter().map(|x| quote(x)).collect::<Vec<_>>().join(", ");
    let cond = if after.is_some() {
        format!("WHERE {} > ?", quote(pk))
    } else {
        String::new()
    };
    let q = format!(
        "SELECT {}, {list} FROM {}.{} {cond} ORDER BY {} LIMIT {n}",
        quote(pk),
        quote(schema),
        quote(table),
        quote(pk)
    );
    let params: Vec<Value> = after.into_iter().collect();
    let rows: Vec<Row> = c
        .exec(q, params)
        .await
        .map_err(|e| db_err("back-fill read", &e))?;
    if rows.is_empty() {
        return Ok((0, None));
    }
    let mut pairs = Vec::new();
    let mut last = None;
    for r in rows {
        let v: Vec<Value> = r.unwrap();
        let h = rk_of(key, &v[1..])?;
        last = Some(v[0].clone());
        pairs.push((v[0].clone(), h));
    }
    c.query_drop(format!("USE {}", quote(schema)))
        .await
        .map_err(|e| db_err("use", &e))?;
    let sql = backfill_columns_sql(table, pk, pairs.len());
    let mut params: Vec<Value> = Vec::with_capacity(pairs.len() * 3);
    for (k, h) in &pairs {
        params.push(k.clone());
        params.push(Value::UInt(*h));
    }
    params.extend(pairs.iter().map(|(k, _)| k.clone()));
    c.exec_drop(sql, params)
        .await
        .map_err(|e| db_err("back-fill write", &e))?;
    Ok((c.affected_rows(), last))
}

async fn set_rk_state(c: &mut Conn, t: TenantId, o: Obj, state: &str) -> Result<()> {
    c.exec_drop(
        format!(
            "UPDATE `{}`.group_members SET rk_state = ?, row_version = row_version + 1 WHERE object_id = ?",
            catalog(t)
        ),
        (state, o),
    )
    .await
    .map_err(|e| db_err("member state", &e))
}

/// Step 3 for one group: `_sprig_rk`, filling, back-fill, ready.
#[allow(clippy::too_many_lines)]
async fn backfill(c: &mut Conn, ctx: &ApplyCtx, g: &CreatedGroup, input: &Input) -> Result<u64> {
    let schema_of = |db: DbId| shard_schema(ctx.tenant, db, g.group, ShardId(1));
    for (o, db, name, _) in &g.members {
        c.query_drop(format!("USE {}", quote(&schema_of(*db))))
            .await
            .map_err(|e| db_err("use", &e))?;
        c.query_drop(add_rk_column(name))
            .await
            .map_err(|e| db_err("add _sprig_rk", &e))?;
        set_rk_state(c, ctx.tenant, *o, "filling").await?;
    }
    if let Some(p) = &ctx.publish {
        p.catalog_changed(ctx.tenant).await?;
    }
    tokio::time::sleep(ctx.fill_settle).await;
    let pk_of = |o: Obj| {
        input
            .tables
            .iter()
            .find(|t| t.object == o)
            .map(|t| t.pk.clone())
            .unwrap_or_default()
    };
    // Parents before children: column members first, then parent members
    // in chain order.
    let mut order: Vec<&(Obj, DbId, String, Routing)> = g
        .members
        .iter()
        .filter(|m| matches!(m.3, Routing::Columns { .. }))
        .collect();
    let mut rest: Vec<&(Obj, DbId, String, Routing)> = g
        .members
        .iter()
        .filter(|m| matches!(m.3, Routing::Parent { .. }))
        .collect();
    while !rest.is_empty() {
        let before = rest.len();
        rest.retain(|m| {
            let Routing::Parent { parent, .. } = &m.3 else {
                return false;
            };
            if order.iter().any(|x| x.0 == *parent) {
                order.push(m);
                false
            } else {
                true
            }
        });
        if rest.len() == before {
            return Err(SprigError::internal(
                "a parent member without its parent in the group",
            ));
        }
    }
    let mut total = 0u64;
    for _pass in 0..3 {
        for (o, db, name, routing) in &order {
            let schema = schema_of(*db);
            let pk = pk_of(*o);
            match routing {
                Routing::Columns { columns } => {
                    if pk.len() != 1 {
                        return Err(SprigError::new(
                            SprigCode::MoveUnsupported,
                            format!("{name}: back-fill needs a one-column primary key"),
                        ));
                    }
                    let mut after = None;
                    loop {
                        let (n, last) = backfill_columns_chunk(
                            c,
                            &schema,
                            name,
                            &pk[0],
                            columns,
                            &g.key,
                            after,
                            ctx.chunk_rows,
                        )
                        .await?;
                        total += n;
                        match last {
                            Some(l) => after = Some(l),
                            None => break,
                        }
                    }
                }
                Routing::Parent { parent, fk } => {
                    let f = input
                        .fks
                        .iter()
                        .find(|f| &f.name == fk && f.child == *o)
                        .ok_or_else(|| {
                            SprigError::internal(format!("{name}: no foreign key {fk}"))
                        })?;
                    let pname = g
                        .members
                        .iter()
                        .find(|m| m.0 == *parent)
                        .map(|m| m.2.clone())
                        .unwrap_or_default();
                    let on: Vec<(String, String)> = f
                        .child_cols
                        .iter()
                        .cloned()
                        .zip(f.parent_cols.iter().cloned())
                        .collect();
                    c.query_drop(format!("USE {}", quote(&schema)))
                        .await
                        .map_err(|e| db_err("use", &e))?;
                    let range = &pk[0];
                    // Chunks in key order: `rk::backfill_parent_sql` takes the
                    // chunk's first and last child key.
                    let sql = backfill_parent_sql(name, &pname, &on, range);
                    let mut after: Option<Value> = None;
                    loop {
                        let cond = if after.is_some() {
                            format!("WHERE {} > ? AND ", quote(range))
                        } else {
                            "WHERE ".to_owned()
                        };
                        let q = format!(
                            "SELECT MIN(k), MAX(k), COUNT(*) FROM (SELECT {0} AS k FROM {1} \
                             {cond}{RK_COLUMN} IS NULL ORDER BY {0} LIMIT {2}) x",
                            quote(range),
                            quote(name),
                            ctx.chunk_rows.max(1)
                        );
                        let params: Vec<Value> = after.iter().cloned().collect();
                        let r: Option<(Value, Value, u64)> = c
                            .exec_first(q, params)
                            .await
                            .map_err(|e| db_err("back-fill chunk", &e))?;
                        let Some((lo, hi, n)) = r else { break };
                        if n == 0 {
                            break;
                        }
                        c.exec_drop(sql.as_str(), vec![lo, hi.clone()])
                            .await
                            .map_err(|e| db_err("back-fill parent", &e))?;
                        total += c.affected_rows();
                        after = Some(hi);
                    }
                }
            }
        }
    }
    // Ready when nothing is NULL and a sample re-hashes correctly.
    for (o, db, name, routing) in &order {
        let schema = schema_of(*db);
        let nulls: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM {}.{} WHERE {RK_COLUMN} IS NULL",
                quote(&schema),
                quote(name)
            ))
            .await
            .map_err(|e| db_err("back-fill check", &e))?;
        if nulls.unwrap_or(0) > 0 {
            return Err(SprigError::new(
                SprigCode::MoveUnsupported,
                format!("{name}: {} rows still lack {RK_COLUMN}", nulls.unwrap_or(0)),
            ));
        }
        if let Routing::Columns { columns } = routing {
            let list = columns
                .iter()
                .map(|x| quote(x))
                .collect::<Vec<_>>()
                .join(", ");
            let rows: Vec<Row> = c
                .exec(
                    format!(
                        "SELECT {RK_COLUMN}, {list} FROM {}.{} LIMIT 1000",
                        quote(&schema),
                        quote(name)
                    ),
                    (),
                )
                .await
                .map_err(|e| db_err("back-fill sample", &e))?;
            for r in rows {
                let v: Vec<Value> = r.unwrap();
                let stored = match &v[0] {
                    Value::UInt(u) => *u,
                    Value::Int(i) => u64::from_ne_bytes(i.to_ne_bytes()),
                    Value::Bytes(b) => String::from_utf8_lossy(b).parse().unwrap_or(0),
                    _ => 0,
                };
                if rk_of(&g.key, &v[1..])? != stored {
                    return Err(SprigError::internal(format!(
                        "{name}: a sampled row's {RK_COLUMN} doesn't re-hash"
                    )));
                }
            }
        }
        set_rk_state(c, ctx.tenant, *o, "ready").await?;
    }
    Ok(total)
}

/// Applies `design` for advisor run `run` (chapter 16's plan, steps 1 to
/// 5). `located` says where each table is now; `maps` is the DC tier's
/// tenant maps, `shard_maps` its group maps.
///
/// # Errors
/// The steps' errors; nothing is lost on any of them, and a re-run
/// resumes.
#[allow(clippy::too_many_arguments)]
pub async fn apply_first_design(
    c: &mut Conn,
    ctx: &ApplyCtx,
    run: u64,
    design: &Design,
    input: &Input,
    located: &[Located],
    maps: &TenantMaps,
    shard_maps: &ShardMapStore,
) -> Result<Applied> {
    let groups = create_groups(c, ctx, run, design, input, located).await?;
    let hold_ms = regroup(c, ctx, run, &groups, located, maps, shard_maps).await?;
    let mut backfilled = 0;
    for g in &groups {
        backfilled += backfill(c, ctx, g, input).await?;
    }
    let cat = catalog(ctx.tenant);
    for g in &groups {
        c.exec_drop(
            format!(
                "UPDATE `{cat}`.table_groups SET state = 'active', updated_at = UTC_TIMESTAMP(6), \
                 row_version = row_version + 1 WHERE group_id = ?"
            ),
            (g.group.0,),
        )
        .await
        .map_err(|e| db_err("activate a group", &e))?;
    }
    if let Some(p) = &ctx.publish {
        p.catalog_changed(ctx.tenant).await?;
    }
    let plan = json!({
        "steps": ["groups", "regroup", "rk", "backfill", "active"],
        "groups": groups.iter().map(|g| json!({
            "group": g.group.0, "name": g.name,
            "members": g.members.iter().map(|m| json!({"object": m.0, "table": m.2})).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "hold_ms": hold_ms,
        "backfilled_rows": backfilled,
    });
    let _: BTreeMap<u8, u8> = BTreeMap::new();
    Ok(Applied {
        groups,
        hold_ms,
        backfilled,
        plan,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sprig_shard::KeyClass;

    #[test]
    fn binary_and_text_protocol_values_hash_alike() {
        let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
        for n in [0i64, 5, -7, 3_000_000_123] {
            let a = rk_of(&spec, &[Value::Int(n)]).unwrap();
            let b = rk_of(&spec, &[Value::Bytes(n.to_string().into_bytes())]).unwrap();
            let c = rk_of(&spec, &[Value::UInt(u64::try_from(n.max(0)).unwrap())]).unwrap();
            assert_eq!(a, b, "{n}");
            if n >= 0 {
                assert_eq!(a, c);
            }
        }
    }
}
