//! Online re-keying (chapter 16, "Online re-key (R3)"; chapter 20): a
//! table group's rows are moved to new shards routed by another key, while
//! clients keep reading and writing.
//!
//! 1. **Checks.** The group is `active`; all its shards are on one
//!    cluster, none held; every member supplies the new key from one
//!    column of its own whose type the router routes (integer, decimal,
//!    `CHAR`/`VARCHAR` under one collation, `BINARY`/`VARBINARY`), the
//!    same class in every member; no member owns a lookup index (its
//!    rows hold the old hash and would need a rebuild).
//! 2. **Next routing.** Each member's `group_members.next_routing` gets the
//!    new key, its column, the first new shard id and the job; the group
//!    becomes `rekeying` (no split or merge touches it) and the catalog
//!    version moves, so gateways load it.
//! 3. **The mover job** (`rekey`, chapter 10): every shard of the group is
//!    a source; the targets are new shards of the same group on one
//!    cluster covering the whole hash space. The mover copies every member
//!    hashing each row again by its new key, catches up from the binlog
//!    the same way, verifies by primary key (every row of each source on
//!    exactly the target its new hash routes to), then cuts over: every
//!    range held, the sources fenced, the last catch-up, and one
//!    compare-and-put of the group's map that replaces all ranges.
//! 4. **Finish** ([`ShardingLoop::rekey_follow`], every tick). Between
//!    the cutover and this step gateways route by the members' next
//!    routing, because every range of the map is a new shard. Once the job
//!    has committed, the catalog's key becomes the new one (`key_spec`,
//!    members `columns` on the new column, `next_routing` cleared, group
//!    `active`), the version moves, and `_sprig_rk_prev` is dropped from
//!    the new shards. A job that ended without committing clears
//!    `next_routing` and returns the group to `active`.
//!
//! The old shards stay fenced for `sharding.retire_hours` (the job's
//! retention), then the mover drops them.

use std::collections::BTreeMap;

use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_balancer::decisions::{Mode, NewDecision, Outcome};
use sprig_common::ids::{ClusterId, DbId, GroupId, JobId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_mover::{HashRange, JobSpec, Mover as _};
use sprig_shard::{KeySpec, Placement, shard_schema};

use super::{ShardingLoop, now_us};

fn not_allowed(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SplitNotAllowed, m)
}

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

/// What `sprigctl shard rekey` asks for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RekeyRequest {
    /// The new key's column, in every member that doesn't name its own.
    pub column: String,
    /// Per member table (by name), its own column for the new key.
    pub members: BTreeMap<String, String>,
    /// How many new shards (default: as many as the group has, at least 2).
    pub shards: Option<u32>,
    /// The cluster for the new shards (default: the group's).
    pub to: Option<String>,
}

/// One member as the re-key sees it.
#[derive(Debug, Clone)]
struct Member {
    object: u64,
    db: u32,
    name: String,
    column: String,
}

/// Even ranges over the hash space, `n` of them.
fn even(n: u32) -> Vec<HashRange> {
    let lo = |i: u32| u64::try_from((u128::from(i) << 64) / u128::from(n)).unwrap_or(u64::MAX);
    (0..n)
        .map(|i| HashRange {
            lo: lo(i),
            hi: (i + 1 < n).then(|| lo(i + 1)),
        })
        .collect()
}

impl ShardingLoop {
    /// Starts an online re-key of `group` (module docs, steps 1 to 3);
    /// returns the mover job.
    ///
    /// # Errors
    /// 39471 when the group or a member can't be re-keyed as asked; 39470
    /// while another job holds the group's shards; store and catalog
    /// errors.
    #[allow(clippy::too_many_lines)] // the checks, in the module's order
    pub async fn rekey(
        &self,
        tenant: TenantId,
        group: u32,
        req: RekeyRequest,
        actor: &str,
    ) -> Result<JobId> {
        let ts = self.settings.settings(tenant);
        if !self.tenant_active(tenant).await? {
            return Err(SprigError::new(
                SprigCode::ShardBusy,
                format!("tenant {tenant} is moving between data centers"),
            ));
        }
        let home = Placement::of(&self.tenant_maps.load(tenant).await?.0)?.cluster;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let row: Option<(String, String)> = c
            .exec_first(
                format!(
                    "SELECT state, CAST(key_spec AS CHAR) FROM `{cat}`.table_groups \
                     WHERE group_id = ? AND kind = 'sharded'"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("table group", &e))?;
        let Some((state, old_spec)) = row else {
            return Err(not_allowed(format!(
                "tenant {tenant} has no sharded table group {group}"
            )));
        };
        if state != "active" {
            return Err(not_allowed(format!("table group {group} is {state}")));
        }
        let members: Vec<(u64, u32, String, String, Option<String>)> = c
            .exec(
                format!(
                    "SELECT m.object_id, m.db_id, o.name, m.routing, CAST(m.key_columns AS CHAR) \
                     FROM `{cat}`.group_members m JOIN `{cat}`.objects o \
                       ON o.object_id = m.object_id \
                     WHERE m.group_id = ? ORDER BY m.object_id"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("members", &e))?;
        if members.is_empty() {
            return Err(not_allowed(format!("table group {group} has no members")));
        }
        let lookups: Vec<(u64, String)> = c
            .exec(
                format!(
                    "SELECT l.object_id, l.index_name FROM `{cat}`.lookup_indexes l \
                     JOIN `{cat}`.group_members m ON m.object_id = l.object_id \
                     WHERE m.group_id = ? AND l.state <> 'dropping'"
                ),
                (group,),
            )
            .await
            .unwrap_or_default();
        if let Some((o, name)) = lookups.first() {
            return Err(not_allowed(format!(
                "object {o} of group {group} owns lookup index {name}: a re-key would have to \
                 rebuild it, which isn't built yet; drop the index first"
            )));
        }
        let old_cols: BTreeMap<u64, Vec<String>> = members
            .iter()
            .filter(|(_, _, _, routing, _)| routing == "columns")
            .filter_map(|(o, _, _, _, cols)| {
                Some((*o, serde_json::from_str(cols.as_deref()?).ok()?))
            })
            .collect();
        let members: Vec<Member> = members
            .into_iter()
            .map(|(object, db, name, _, _)| Member {
                column: req
                    .members
                    .iter()
                    .find(|(t, _)| t.eq_ignore_ascii_case(&name))
                    .map_or_else(|| req.column.clone(), |(_, c)| c.clone()),
                object,
                db,
                name,
            })
            .collect();
        if let Some(m) = members.iter().find(|m| m.column.is_empty()) {
            return Err(not_allowed(format!(
                "member {} names no column for the new key",
                m.name
            )));
        }
        // The map: every shard on one cluster, none held.
        let gid = GroupId(group);
        let (map, _) = self.maps.load(tenant, gid).await?;
        let from = map
            .ranges
            .first()
            .map(|r| r.cluster.clone())
            .ok_or_else(|| not_allowed(format!("group {group}'s map has no ranges")))?;
        if map.ranges.iter().any(|r| r.cluster != from) {
            return Err(not_allowed(format!(
                "group {group}'s shards are on several clusters; move them to one cluster first \
                 (a re-key reads one cluster's binlog)"
            )));
        }
        if let Some(r) = map
            .ranges
            .iter()
            .find(|r| r.state == sprig_shard::shardmap::RangeState::Held)
        {
            return Err(SprigError::new(
                SprigCode::ShardBusy,
                format!("shard {} of group {group} is held by {:?}", r.shard, r.job_id),
            ));
        }
        let to = match &req.to {
            Some(t) => ClusterId::new(t).map_err(|e| not_allowed(format!("--to {t}: {e}")))?,
            None => from.clone(),
        };
        let dbs: Vec<DbId> = {
            let mut v: Vec<u32> = members.iter().map(|m| m.db).collect();
            v.sort_unstable();
            v.dedup();
            v.into_iter().map(DbId).collect()
        };
        // The new key's class, from the first shard's columns.
        let mut sc = self.conn(&from).await?;
        let first = map.ranges[0].shard;
        let mut class: Option<sprig_shard::KeyClass> = None;
        for m in &members {
            let schema = shard_schema(tenant, DbId(m.db), gid, first);
            let col: Option<(String, Option<String>)> = sc
                .exec_first(
                    "SELECT DATA_TYPE, COLLATION_NAME FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                    (schema.as_str(), m.name.as_str(), m.column.as_str()),
                )
                .await
                .map_err(|e| db_err("columns", &e))?;
            let Some((ty, coll)) = col else {
                return Err(not_allowed(format!(
                    "member {} has no column {} for the new key",
                    m.name, m.column
                )));
            };
            let k = sprig_shard::pins::column_class(&ty, coll.as_deref())
                .filter(|k| {
                    matches!(
                        k,
                        sprig_shard::KeyClass::Numeric
                            | sprig_shard::KeyClass::String { .. }
                            | sprig_shard::KeyClass::Binary
                    )
                })
                .ok_or_else(|| {
                    not_allowed(format!(
                        "{}.{} ({ty}) can't be a distribution key the router routes",
                        m.name, m.column
                    ))
                })?;
            match class {
                None => class = Some(k),
                Some(c) if c == k => {}
                Some(c) => {
                    return Err(not_allowed(format!(
                        "{}.{} is {} but the new key is {}: every member's column must \
                         compare the same way",
                        m.name,
                        m.column,
                        sprig_shard::pins::class_name(k),
                        sprig_shard::pins::class_name(c)
                    )));
                }
            }
        }
        drop(sc);
        let key = KeySpec::new(vec![class.unwrap_or(sprig_shard::KeyClass::Numeric)])?;
        // A key the group already has isn't a re-key.
        if members.iter().all(|m| {
            old_cols
                .get(&m.object)
                .is_some_and(|c| c.len() == 1 && c[0].eq_ignore_ascii_case(&m.column))
        }) {
            return Err(not_allowed(format!(
                "group {group} is already keyed by these columns"
            )));
        }
        let old: Option<KeySpec> = serde_json::from_str::<Value>(&old_spec)
            .ok()
            .and_then(|v| KeySpec::from_json(&v).ok());
        if old.as_ref().is_some_and(|k| k.parts.len() != 1) {
            return Err(not_allowed(format!(
                "group {group} has a key of several parts; a re-key moves single-column keys"
            )));
        }
        let (running, _) = self.follow(tenant).await?;
        if running
            .iter()
            .any(|r| r.tenant == tenant && r.group == Some(group))
        {
            return Err(SprigError::new(
                SprigCode::ShardBusy,
                format!("a job of group {group} runs; retry when it finishes"),
            ));
        }
        let ddl_rows = sprig_catalog::store::Store::for_tenant(tenant)
            .settings_of(&mut c, sprig_catalog::store::SettingScope::Tenant)
            .await?;
        let source_schemas: Vec<SchemaName> = map
            .ranges
            .iter()
            .flat_map(|r| {
                dbs.iter()
                    .map(move |d| SchemaName::shard(tenant, *d, gid, r.shard))
            })
            .collect();
        if !sprig_catalog::ddl::jobs_on_shards(&ddl_rows, &source_schemas).is_empty() {
            return Err(SprigError::new(
                SprigCode::ShardBusy,
                format!("a schema change holds group {group}; retry when it finishes"),
            ));
        }
        let n = req
            .shards
            .unwrap_or_else(|| u32::try_from(map.ranges.len()).unwrap_or(2).max(2))
            .clamp(1, 256);
        let next = sprig_balancer::shards::next_shard_id(&map, &running);
        let sources: Vec<(u32, Vec<String>, HashRange)> = map
            .ranges
            .iter()
            .enumerate()
            .map(|(i, r)| {
                (
                    r.shard.0,
                    dbs.iter()
                        .map(|d| shard_schema(tenant, *d, gid, r.shard))
                        .collect(),
                    HashRange {
                        lo: r.lo,
                        hi: map.hi(i),
                    },
                )
            })
            .collect();
        let targets: Vec<(u32, HashRange)> = even(n)
            .into_iter()
            .enumerate()
            .map(|(i, r)| (next + u32::try_from(i).unwrap_or(0), r))
            .collect();
        let columns: BTreeMap<String, Vec<String>> = members
            .iter()
            .map(|m| (m.name.clone(), vec![m.column.clone()]))
            .collect();
        let retire = u64::try_from(ts.sharding.retire.as_millis()).unwrap_or(u64::MAX);
        let mut spec = JobSpec::rekey(
            tenant,
            group,
            &from,
            sources,
            &to,
            targets,
            key.to_json(),
            columns,
        );
        spec.retain_ms = Some(retire);
        spec.requested_by = Some(actor.to_owned());
        let from_cols: Vec<String> = members
            .iter()
            .map(|m| {
                old_cols.get(&m.object).map_or_else(
                    || format!("{} (by its parent)", m.name),
                    |c| format!("{}.{}", m.name, c.join(",")),
                )
            })
            .collect();
        let summary = format!(
            "re-key group {group} from {} to {} ({} shard(s) into {n} new from shard {next})",
            from_cols.join(", "),
            members
                .iter()
                .map(|m| format!("{}.{}", m.name, m.column))
                .collect::<Vec<_>>()
                .join(", "),
            map.ranges.len()
        );
        let d = self
            .record(
                tenant,
                &NewDecision {
                    decided_at_us: now_us(),
                    component: "advisor".into(),
                    kind: "group.rekey".into(),
                    subject_type: "group".into(),
                    subject_id: format!("t{tenant}/g{group}"),
                    mode: Mode::Applied,
                    outcome: Outcome::Proposed,
                    reason_code: "manual".into(),
                    summary: summary.chars().take(512).collect(),
                    evidence: json!({
                        "requested_by": actor,
                        "key": key.to_json(),
                        "columns": members.iter().map(|m| json!([m.name, m.column])).collect::<Vec<_>>(),
                        "shards": n,
                        "from": from.to_string(),
                        "to": to.to_string(),
                    }),
                    alternatives: None,
                    job_id: None,
                    parent_id: None,
                    actor: self.actor.clone(),
                    dc_id: self.dc.clone(),
                },
            )
            .await;
        if d > 0 {
            spec = spec.with_decision(d);
        }
        // Step 2: next routing, then the job. The job id isn't known before
        // the submit; the members carry the first new shard, which is
        // enough to tell the cutover happened, and the follow step finds
        // the job by group and kind.
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(|e| db_err("begin", &e))?;
        for m in &members {
            let next_routing = json!({
                "key_spec": key.to_json(),
                "key_columns": [m.column],
                "shard_from": next,
            });
            tx.exec_drop(
                format!(
                    "UPDATE `{cat}`.group_members SET next_routing = ?, \
                     row_version = row_version + 1 WHERE object_id = ?"
                ),
                (next_routing.to_string(), m.object),
            )
            .await
            .map_err(|e| db_err("next routing", &e))?;
        }
        tx.exec_drop(
            format!(
                "UPDATE `{cat}`.table_groups SET state = 'rekeying', updated_at = UTC_TIMESTAMP(6), \
                 row_version = row_version + 1 WHERE group_id = ? AND state = 'active'"
            ),
            (group,),
        )
        .await
        .map_err(|e| db_err("group state", &e))?;
        tx.commit().await.map_err(|e| db_err("commit", &e))?;
        drop(c);
        if let Some(p) = &self.publish {
            p.catalog_changed(tenant).await?;
        }
        match self.mover.submit(spec).await {
            Ok(id) => {
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Scheduled, now_us())
                        .await;
                }
                metrics::counter!("sprig_advisor_rekeys_total", "outcome" => "started")
                    .increment(1);
                tracing::info!(tenant = %tenant, group, job = %id.0, "re-key submitted");
                Ok(id)
            }
            Err(e) => {
                if d > 0 {
                    let _ = self
                        .decisions
                        .update_outcome(tenant, d, Outcome::Rejected, now_us())
                        .await;
                }
                // Undo step 2.
                let _ = self.rekey_undo(tenant, group).await;
                Err(e)
            }
        }
    }

    /// Clears a re-key's next routing and returns the group to `active`.
    async fn rekey_undo(&self, tenant: TenantId, group: u32) -> Result<()> {
        let home = Placement::of(&self.tenant_maps.load(tenant).await?.0)?.cluster;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        c.exec_drop(
            format!(
                "UPDATE `{cat}`.group_members SET next_routing = NULL, \
                 row_version = row_version + 1 WHERE group_id = ? AND next_routing IS NOT NULL"
            ),
            (group,),
        )
        .await
        .map_err(|e| db_err("next routing", &e))?;
        c.exec_drop(
            format!(
                "UPDATE `{cat}`.table_groups SET state = 'active', updated_at = UTC_TIMESTAMP(6), \
                 row_version = row_version + 1 WHERE group_id = ? AND state = 'rekeying'"
            ),
            (group,),
        )
        .await
        .map_err(|e| db_err("group state", &e))?;
        drop(c);
        if let Some(p) = &self.publish {
            p.catalog_changed(tenant).await?;
        }
        Ok(())
    }

    /// Step 4 for every `rekeying` group of `tenant`: finishes a committed
    /// re-key's catalog, or undoes one whose job ended without committing.
    /// Returns `(group, "finished" | "undone" | "running")`.
    ///
    /// # Errors
    /// Store and catalog errors (the next tick retries).
    pub async fn rekey_follow(&self, tenant: TenantId) -> Result<Vec<(u32, &'static str)>> {
        let home = Placement::of(&self.tenant_maps.load(tenant).await?.0)?.cluster;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let groups: Vec<u32> = c
            .query(format!(
                "SELECT group_id FROM `{cat}`.table_groups WHERE state = 'rekeying' \
                 ORDER BY group_id"
            ))
            .await
            .map_err(|e| db_err("table groups", &e))?;
        let mut out = Vec::new();
        if groups.is_empty() {
            return Ok(out);
        }
        let jobs = self.jobs.all().await?;
        for g in groups {
            let next: Vec<(u64, String, String)> = c
                .exec(
                    format!(
                        "SELECT m.object_id, o.name, CAST(m.next_routing AS CHAR) \
                         FROM `{cat}`.group_members m JOIN `{cat}`.objects o \
                           ON o.object_id = m.object_id \
                         WHERE m.group_id = ? AND m.next_routing IS NOT NULL"
                    ),
                    (g,),
                )
                .await
                .map_err(|e| db_err("next routing", &e))?;
            let from = next
                .iter()
                .filter_map(|(_, _, j)| serde_json::from_str::<Value>(j).ok())
                .filter_map(|v| v.get("shard_from").and_then(Value::as_u64))
                .max()
                .unwrap_or(0);
            // The newest re-key job of the group whose targets start there.
            let job = jobs
                .iter()
                .map(|(r, _)| r)
                .filter(|r| {
                    r.tenant_id == tenant
                        && r.kind == sprig_mover::JobKind::Rekey
                        && r.spec.group() == Some(g)
                        && r.spec.targets.iter().map(|t| u64::from(t.shard)).min() == Some(from)
                })
                .max_by_key(|r| r.created_ms);
            let (map, _) = self.maps.load(tenant, GroupId(g)).await?;
            let cut = from > 0
                && map
                    .ranges
                    .iter()
                    .all(|r| u64::from(r.shard.0) >= from);
            let ended = job.is_none_or(|r| r.state.is_terminal());
            if cut {
                self.rekey_finish(tenant, g, &next, &map).await?;
                metrics::counter!("sprig_advisor_rekeys_total", "outcome" => "finished")
                    .increment(1);
                out.push((g, "finished"));
            } else if ended && job.is_none_or(|r| !r.state.is_committed()) {
                tracing::warn!(tenant = %tenant, group = g, state = ?job.map(|r| r.state), "re-key ended without a cutover; undoing");
                self.rekey_undo(tenant, g).await?;
                metrics::counter!("sprig_advisor_rekeys_total", "outcome" => "rolled_back")
                    .increment(1);
                out.push((g, "undone"));
            } else {
                out.push((g, "running"));
            }
        }
        Ok(out)
    }

    /// The catalog after a committed re-key (module docs, step 4).
    async fn rekey_finish(
        &self,
        tenant: TenantId,
        group: u32,
        next: &[(u64, String, String)],
        map: &sprig_shard::ShardMap,
    ) -> Result<()> {
        let home = Placement::of(&self.tenant_maps.load(tenant).await?.0)?.cluster;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let mut key: Option<KeySpec> = None;
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(|e| db_err("begin", &e))?;
        for (object, _, json) in next {
            let v: Value = serde_json::from_str(json)
                .map_err(|e| SprigError::internal(format!("next routing of {object}: {e}")))?;
            let spec = v
                .get("key_spec")
                .map(KeySpec::from_json)
                .transpose()?
                .ok_or_else(|| SprigError::internal(format!("next routing of {object}")))?;
            let cols = v.get("key_columns").cloned().unwrap_or_else(|| json!([]));
            tx.exec_drop(
                format!(
                    "UPDATE `{cat}`.group_members SET routing = 'columns', key_columns = ?, \
                     parent_object_id = NULL, parent_fk = NULL, rk_state = 'ready', \
                     next_routing = NULL, row_version = row_version + 1 WHERE object_id = ?"
                ),
                (cols.to_string(), *object),
            )
            .await
            .map_err(|e| db_err("member routing", &e))?;
            key = Some(spec);
        }
        if let Some(k) = &key {
            tx.exec_drop(
                format!(
                    "UPDATE `{cat}`.table_groups SET key_spec = ?, key_spec_digest = ?, \
                     state = 'active', updated_at = UTC_TIMESTAMP(6), \
                     row_version = row_version + 1 WHERE group_id = ?"
                ),
                (k.to_json().to_string(), k.digest, group),
            )
            .await
            .map_err(|e| db_err("group key", &e))?;
        } else {
            tx.exec_drop(
                format!(
                    "UPDATE `{cat}`.table_groups SET state = 'active', \
                     updated_at = UTC_TIMESTAMP(6), row_version = row_version + 1 \
                     WHERE group_id = ?"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("group state", &e))?;
        }
        tx.commit().await.map_err(|e| db_err("commit", &e))?;
        let dbs: Vec<u32> = c
            .exec(
                format!(
                    "SELECT DISTINCT db_id FROM `{cat}`.group_members WHERE group_id = ? \
                     ORDER BY db_id"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("members", &e))?;
        drop(c);
        if let Some(p) = &self.publish {
            p.catalog_changed(tenant).await?;
        }
        // The verification column goes (an instant DROP COLUMN; a failure
        // leaves an invisible column behind, nothing else).
        for r in &map.ranges {
            let Ok(mut sc) = self.conn(&r.cluster).await else {
                continue;
            };
            for d in &dbs {
                let schema = shard_schema(tenant, DbId(*d), GroupId(group), ShardId(r.shard.0));
                let tables: Vec<String> = sc
                    .exec(
                        "SELECT TABLE_NAME FROM information_schema.COLUMNS \
                         WHERE TABLE_SCHEMA = ? AND COLUMN_NAME = ?",
                        (schema.as_str(), sprig_mover::route::RK_PREV_COLUMN),
                    )
                    .await
                    .unwrap_or_default();
                for t in tables {
                    if let Err(e) = sc
                        .query_drop(format!(
                            "ALTER TABLE `{schema}`.`{}` DROP COLUMN IF EXISTS {}",
                            t.replace('`', "``"),
                            sprig_mover::route::RK_PREV_COLUMN
                        ))
                        .await
                    {
                        tracing::warn!(schema = %schema, table = %t, error = %e, "re-key: _sprig_rk_prev not dropped");
                    }
                }
            }
        }
        tracing::info!(tenant = %tenant, group, "re-key finished: the catalog has the new key");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn even_ranges_cover_the_space() {
        let r = even(3);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].lo, 0);
        assert_eq!(r[0].hi, Some(r[1].lo));
        assert_eq!(r[1].hi, Some(r[2].lo));
        assert_eq!(r[2].hi, None);
        assert_eq!(even(1)[0], HashRange { lo: 0, hi: None });
    }
}
