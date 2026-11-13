//! Operator calls on the sharding loop (chapter 20's `sprigctl shard`,
//! chapter 16's `sprigctl advisor`), behind the Admin API.
//!
//! Manual jobs skip the triggers but obey the same exclusion as automatic
//! ones: a schema change holding a shard, a whole-tenant job, a running
//! job on the shard, `sharding.max_jobs_per_tenant` and the copy slots
//! refuse them with 39470 (they don't wait). `sharding.mode = recommend`
//! and failure backoff don't apply: the operator asked.

use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_advisor::sharding::design::{Design, GroupDesign, Member};
use sprig_advisor::sharding::{input, runs};
use sprig_balancer::shards::{
    self, Admission, Backoff, Proposal, Reason, ShardJob, ShardingMode, Weight,
};
use sprig_common::ids::{ClusterId, DbId, GroupId, JobId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_mover::Mover as _;
use sprig_shard::{Placement, shard_schema};

use super::{ShardingLoop, TickReport, now_ms};

/// A manual shard job (`sprigctl shard split|merge|move`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ManualJob {
    /// Split `shard` at `at`, or into `ways` children at the size
    /// histogram's quantiles (2 by default).
    Split {
        /// The shard.
        shard: u32,
        /// One split point.
        #[serde(default)]
        at: Option<u64>,
        /// Children, 2 to `sharding.max_split_ways`.
        #[serde(default)]
        ways: Option<u32>,
    },
    /// Merge adjacent `left` and `right`.
    Merge {
        /// The lower range.
        left: u32,
        /// The upper range.
        right: u32,
    },
    /// Move `shard` to cluster `to`.
    Move {
        /// The shard.
        shard: u32,
        /// The target cluster.
        to: String,
    },
    /// Re-key the whole group online (chapter 16, R3): every member moves
    /// to new shards routed by `column` (or its own column in `members`).
    Rekey {
        /// The new key's column, in every member not in `members`.
        column: String,
        /// Per member table name, its own column for the new key.
        #[serde(default)]
        members: std::collections::BTreeMap<String, String>,
        /// How many new shards (default: as many as now, at least 2).
        #[serde(default)]
        shards: Option<u32>,
        /// The cluster for the new shards (default: the group's).
        #[serde(default)]
        to: Option<String>,
    },
}

/// One range of a group's map, as `sprigctl shard list` shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardRow {
    /// The shard id.
    pub shard: u32,
    /// First hash, hex.
    pub lo: String,
    /// One past the last, hex; `None` is 2^64.
    pub hi: Option<String>,
    /// Its cluster.
    pub cluster: String,
    /// `serving` or `held`.
    pub state: String,
    /// Its epoch.
    pub epoch: u64,
    /// The job holding it, if any.
    pub job: Option<String>,
}

/// One table group with its map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupView {
    /// The group id.
    pub group: u32,
    /// Its name (the root table's).
    pub name: String,
    /// `preparing`, `active`, ...
    pub state: String,
    /// The map's version.
    pub map_version: u64,
    /// Ranges, by hash.
    pub shards: Vec<ShardRow>,
}

fn busy(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ShardBusy, m)
}

fn not_allowed(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SplitNotAllowed, m)
}

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

impl ShardingLoop {
    async fn home(&self, tenant: TenantId) -> Result<ClusterId> {
        let (tm, _) = self.tenant_maps.load(tenant).await?;
        Ok(Placement::of(&tm)?.cluster)
    }

    /// The tenant's table groups and their maps.
    ///
    /// # Errors
    /// Store and catalog errors.
    pub async fn shards(&self, tenant: TenantId) -> Result<Vec<GroupView>> {
        let home = self.home(tenant).await?;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        sprig_shard::groups::migrate(&mut c, &cat).await?;
        let groups: Vec<(u32, String, String)> = c
            .query(format!(
                "SELECT group_id, name, state FROM `{cat}`.table_groups \
                 WHERE kind = 'sharded' ORDER BY group_id"
            ))
            .await
            .map_err(|e| db_err("table groups", &e))?;
        let mut out = Vec::new();
        for (g, name, state) in groups {
            let Ok((m, _)) = self.maps.load(tenant, GroupId(g)).await else {
                out.push(GroupView {
                    group: g,
                    name,
                    state,
                    map_version: 0,
                    shards: Vec::new(),
                });
                continue;
            };
            let shards = m
                .ranges
                .iter()
                .enumerate()
                .map(|(i, r)| ShardRow {
                    shard: r.shard.0,
                    lo: format!("{:#018x}", r.lo),
                    hi: m.hi(i).map(|h| format!("{h:#018x}")),
                    cluster: r.cluster.to_string(),
                    state: format!("{:?}", r.state).to_lowercase(),
                    epoch: r.epoch,
                    job: r.job_id.clone(),
                })
                .collect();
            out.push(GroupView {
                group: g,
                name,
                state,
                map_version: m.version,
                shards,
            });
        }
        Ok(out)
    }

    /// Submits a manual split, merge or shard move of `group` (module
    /// docs); returns the mover job.
    ///
    /// # Errors
    /// 39471 for a group that isn't active or a job that doesn't fit the
    /// map; 39470 while something else holds the shards; store errors.
    #[allow(clippy::too_many_lines)] // checks, then the same path as the loop
    pub async fn manual(
        &self,
        tenant: TenantId,
        group: u32,
        job: ManualJob,
        actor: &str,
    ) -> Result<JobId> {
        if let ManualJob::Rekey {
            column,
            members,
            shards,
            to,
        } = job
        {
            return self
                .rekey(
                    tenant,
                    group,
                    super::RekeyRequest {
                        column,
                        members,
                        shards,
                        to,
                    },
                    actor,
                )
                .await;
        }
        let mut ts = self.settings.settings(tenant);
        if !self.tenant_active(tenant).await? {
            return Err(busy(format!(
                "tenant {tenant} is moving between data centers"
            )));
        }
        let home = self.home(tenant).await?;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let state: Option<String> = c
            .exec_first(
                format!(
                    "SELECT state FROM `{cat}`.table_groups WHERE group_id = ? AND kind = 'sharded'"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("table group", &e))?;
        match state.as_deref() {
            Some("active") => {}
            Some(s) => return Err(not_allowed(format!("table group {group} is {s}"))),
            None => {
                return Err(not_allowed(format!(
                    "tenant {tenant} has no table group {group}"
                )));
            }
        }
        let dbs: Vec<u32> = c
            .exec(
                format!(
                    "SELECT DISTINCT db_id FROM `{cat}`.group_members WHERE group_id = ? ORDER BY db_id"
                ),
                (group,),
            )
            .await
            .map_err(|e| db_err("members", &e))?;
        let ddl_rows = sprig_catalog::store::Store::for_tenant(tenant)
            .settings_of(&mut c, sprig_catalog::store::SettingScope::Tenant)
            .await?;
        drop(c);
        let dbs: Vec<DbId> = dbs.into_iter().map(DbId).collect();
        let gid = GroupId(group);
        let (map, _) = self.maps.load(tenant, gid).await?;
        let range = |shard: u32| -> Result<(usize, &sprig_shard::RangeEntry)> {
            map.ranges
                .iter()
                .enumerate()
                .find(|(_, r)| r.shard == ShardId(shard))
                .ok_or_else(|| not_allowed(format!("shard {shard} isn't in group {group}'s map")))
        };
        let schemas = |s: u32| -> Vec<String> {
            dbs.iter()
                .map(|d| shard_schema(tenant, *d, gid, ShardId(s)))
                .collect()
        };
        let mut evidence = json!({ "requested_by": actor });
        let sj = match job {
            ManualJob::Split { shard, at, ways } => {
                let (i, r) = range(shard)?;
                let (lo, hi) = (r.lo, map.hi(i));
                let points = if let Some(at) = at {
                    if at <= lo || hi.is_some_and(|h| at >= h) {
                        return Err(not_allowed(format!(
                            "{at:#018x} isn't inside shard {shard}'s range"
                        )));
                    }
                    vec![at]
                } else {
                    let ways = ways
                        .unwrap_or(2)
                        .clamp(2, ts.sharding.max_split_ways.max(2));
                    let mut cc = self.conn(&r.cluster).await?;
                    let size = shards::stats::shard_size(&mut cc, &schemas(shard)).await?;
                    let h = shards::stats::size_histogram(&mut cc, &schemas(shard), lo, hi, size)
                        .await
                        .unwrap_or_else(|_| shards::HashHistogram::equal(lo, hi, 64));
                    let plan = shards::choose_split(&h, Weight::Bytes, ways).map_err(|why| {
                        not_allowed(format!("no split of shard {shard}: {why:?}"))
                    })?;
                    if let Some(o) = evidence.as_object_mut() {
                        o.insert("ways".into(), json!(ways));
                        o.insert("shares".into(), json!(plan.shares));
                        o.insert("size_bytes".into(), json!(size));
                    }
                    plan.points
                };
                ShardJob::Split { shard, points }
            }
            ManualJob::Merge { left, right } => {
                range(left)?;
                range(right)?;
                ShardJob::Merge { left, right }
            }
            ManualJob::Move { shard, to } => {
                let (_, r) = range(shard)?;
                let to = ClusterId::new(&to).map_err(|e| not_allowed(format!("--to {to}: {e}")))?;
                if r.cluster == to {
                    return Err(not_allowed(format!("shard {shard} is already on {to}")));
                }
                // The target must be reachable before a job is created.
                drop(self.conn(&to).await?);
                ShardJob::Move { shard, to }
            }
            ManualJob::Rekey { .. } => {
                return Err(not_allowed("a re-key isn't a shard job"));
            }
        };
        let p = Proposal {
            tenant,
            group,
            job: sj,
            reason: Reason::Manual,
            evidence,
        };
        let sources = p.job.sources();
        let source_schemas: Vec<SchemaName> = sources
            .iter()
            .flat_map(|x| {
                dbs.iter()
                    .map(move |d| SchemaName::shard(tenant, *d, gid, ShardId(*x)))
            })
            .collect();
        if !sprig_catalog::ddl::jobs_on_shards(&ddl_rows, &source_schemas).is_empty() {
            return Err(busy(format!(
                "a schema change holds shard(s) {sources:?} of group {group}; retry when it finishes"
            )));
        }
        let (mut running, _) = self.follow(tenant).await?;
        let cluster = range(sources[0])?.1.cluster.clone();
        ts.sharding.mode = ShardingMode::On;
        match shards::admit(
            &p,
            &running,
            &ts.sharding,
            &cluster,
            &Backoff::default(),
            now_ms(),
        ) {
            Admission::Submit | Admission::Recommend => {}
            Admission::Wait(why) => {
                return Err(busy(format!(
                    "{} of shard(s) {sources:?} of group {group} can't start now: {why}",
                    p.job.kind()
                )));
            }
        }
        let mut report = TickReport::default();
        let id = self
            .submit(tenant, &map, &dbs, &p, &ts, &mut running, &mut report)
            .await??;
        if let Some(why) = self
            .late_conflict(tenant, gid, &sources, &dbs, &home)
            .await?
        {
            let _ = self.mover.cancel(&id, &format!("withdrawn: {why}")).await;
            return Err(busy(format!(
                "{} of group {group} withdrawn: {why} started meanwhile",
                p.job.kind()
            )));
        }
        Ok(id)
    }

    /// Runs the advisor now (`sprigctl advisor run`, cause `manual`) for a
    /// tenant without table groups; its verdict follows the guardrails, so
    /// it may apply the design.
    ///
    /// # Errors
    /// 39471 for a tenant that already has table groups (re-keys are R3);
    /// store and catalog errors.
    pub async fn advisor_run(&self, tenant: TenantId) -> Result<TickReport> {
        let ts = self.settings.settings(tenant);
        let home = self.home(tenant).await?;
        let mut c = self.conn(&home).await?;
        self.no_groups(tenant, &mut c).await?;
        let mut report = TickReport::default();
        self.first_design(tenant, &ts, &home, &mut c, &mut report, true)
            .await?;
        Ok(report)
    }

    async fn no_groups(&self, tenant: TenantId, c: &mut mysql_async::Conn) -> Result<()> {
        let cat = SchemaName::catalog(tenant).to_string();
        sprig_shard::groups::migrate(c, &cat).await?;
        let n: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM `{cat}`.table_groups WHERE kind = 'sharded'"
            ))
            .await
            .map_err(|e| db_err("table groups", &e))?;
        if n.unwrap_or(0) > 0 {
            return Err(not_allowed(format!(
                "tenant {tenant} already has table groups: the advisor designs only a tenant's \
                 first groups; change existing ones with `sprigctl shard rekey`"
            )));
        }
        Ok(())
    }

    /// The advisor run `run`, or the newest one, as JSON (`sprigctl
    /// advisor show`); `None` if there is none.
    ///
    /// # Errors
    /// Store and catalog errors.
    #[allow(clippy::many_single_char_names)] // column readers by type
    pub async fn advisor_show(&self, tenant: TenantId, run: Option<u64>) -> Result<Option<Value>> {
        let home = self.home(tenant).await?;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        runs::migrate(&mut c, &cat).await?;
        let sql = format!(
            "SELECT run_id, cause, mode, status, verdict, units_sampled, best_score, \
             runner_up_score, confidence, CAST(design AS CHAR), CAST(evidence AS CHAR), \
             CAST(plan AS CHAR), decision_id, error, \
             DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), \
             DATE_FORMAT(finished_at, '%Y-%m-%d %H:%i:%s') FROM `{cat}`.advisor_runs {} \
             ORDER BY run_id DESC LIMIT 1",
            if run.is_some() {
                "WHERE run_id = ?"
            } else {
                ""
            }
        );
        let params: Vec<mysql_async::Value> = run.into_iter().map(Into::into).collect();
        let row: Option<mysql_async::Row> = c
            .exec_first(sql, params)
            .await
            .map_err(|e| db_err("advisor run", &e))?;
        let Some(r) = row else { return Ok(None) };
        let j = |i: usize| -> Value {
            r.get_opt::<Option<String>, _>(i)
                .and_then(std::result::Result::ok)
                .flatten()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(Value::Null)
        };
        let s = |i: usize| -> Option<String> {
            r.get_opt::<Option<String>, _>(i)
                .and_then(std::result::Result::ok)
                .flatten()
        };
        let f = |i: usize| -> Option<f64> {
            r.get_opt::<Option<f64>, _>(i)
                .and_then(std::result::Result::ok)
                .flatten()
        };
        let u = |i: usize| -> Option<u64> {
            r.get_opt::<Option<u64>, _>(i)
                .and_then(std::result::Result::ok)
                .flatten()
        };
        Ok(Some(json!({
            "run_id": u(0), "cause": s(1), "mode": s(2), "status": s(3), "verdict": s(4),
            "units_sampled": u(5), "best_score": f(6), "runner_up_score": f(7),
            "confidence": f(8), "design": j(9), "evidence": j(10), "plan": j(11),
            "decision_id": u(12), "error": s(13), "created_at": s(14), "finished_at": s(15),
        })))
    }

    /// Applies run `run`'s design (`sprigctl advisor apply`, for a
    /// `recommend` or `wait` verdict) to a tenant without table groups.
    ///
    /// # Errors
    /// 39471 for a run that isn't `decided`, has no design, or a tenant that
    /// already has groups; 39470 while a whole-tenant job holds the tenant;
    /// store and catalog errors.
    pub async fn advisor_apply(&self, tenant: TenantId, run: u64) -> Result<TickReport> {
        let ts = self.settings.settings(tenant);
        let home = self.home(tenant).await?;
        let mut c = self.conn(&home).await?;
        self.no_groups(tenant, &mut c).await?;
        let shown = self
            .advisor_show(tenant, Some(run))
            .await?
            .ok_or_else(|| not_allowed(format!("tenant {tenant} has no advisor run {run}")))?;
        if shown["status"].as_str() != Some("decided") {
            return Err(not_allowed(format!(
                "advisor run {run} is {}, not decided",
                shown["status"].as_str().unwrap_or("?")
            )));
        }
        let design = design_from_json(&shown["design"])?;
        if design.groups.is_empty() {
            return Err(not_allowed(format!(
                "advisor run {run} has no table groups to apply"
            )));
        }
        let d = shown["decision_id"].as_u64().unwrap_or(0);
        let asm = input::assemble(
            &mut c,
            tenant,
            std::time::SystemTime::now(),
            ts.advisor.window,
            ts.sharding.split_bytes,
        )
        .await?;
        let mut report = TickReport {
            advisor: Some((run, "apply")),
            ..TickReport::default()
        };
        self.apply_run(
            tenant,
            &ts,
            &home,
            &mut c,
            run,
            d,
            &design,
            &asm,
            &mut report,
        )
        .await?;
        if report.applied_hold_ms.is_none() {
            return Err(busy(format!(
                "advisor run {run} wasn't applied: a job holds tenant {tenant}; retry"
            )));
        }
        Ok(report)
    }
}

/// A design back from `advisor_runs.design` (`Design::to_json`).
///
/// # Errors
/// `Internal` for JSON that isn't a stored design.
pub fn design_from_json(v: &Value) -> Result<Design> {
    let bad = |m: &str| SprigError::internal(format!("stored design: {m}"));
    let mut d = Design::default();
    for g in v["groups"].as_array().ok_or_else(|| bad("no groups"))? {
        let key = sprig_shard::KeySpec::from_json(&g["key"])?;
        let members = g["members"]
            .as_array()
            .ok_or_else(|| bad("no members"))?
            .iter()
            .map(|m| {
                Ok(Member {
                    object: m["object"].as_u64().ok_or_else(|| bad("member object"))?,
                    routing: serde_json::from_value(m["routing"].clone())
                        .map_err(|e| bad(&e.to_string()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        d.groups.push(GroupDesign {
            name: g["name"].as_str().unwrap_or_default().to_owned(),
            key: key.parts.clone(),
            members,
        });
    }
    for u in v["unsharded"].as_array().into_iter().flatten() {
        if let Some(o) = u["object"].as_u64() {
            d.unsharded.push(o);
        }
    }
    Ok(d)
}
