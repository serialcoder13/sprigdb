//! Scheduled jobs (chapter 41, "Scheduled jobs"; R7 order 5).
//!
//! ```text
//! deploy: logic_schedules row (next_fire_at from the cron text in its zone)
//!         + index logic/schedules/<tenant>/<module> in the DC tier, so the
//!         leader finds due schedules without reading every catalog
//! leader, every second, for each index entry due now (tick):
//!   running_job_id names a job that is gone or finished → clear it (error)
//!   running_job_id live, fire due → skipped (or queued, OVERLAP QUEUE)
//!   queued and nothing running   → fire the queued run now
//!   late by more than logic.schedule_misfire_seconds → misfired
//!   else, compare-and-set in the catalog: running_job_id NULL → a new
//!        job id, next_fire_at → the next cron time; then submit worker job
//!        logic_schedule with that id
//! worker (run): only if running_job_id is still its own job id: CALL the
//!   schedule's routine under logic.job_budget_ms; running_job_id := NULL,
//!   last_outcome; a queued run makes the index due at once
//! ```
//!
//! The compare-and-set in the catalog is what makes one run per fire: a
//! second leader (after a failover) or a second tick finds the slot taken
//! or `next_fire_at` moved, and a job whose id isn't in the slot (a
//! duplicate claim, a submit retried) exits without running.

use std::time::Duration;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_common::ids::{JobId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};
use sprig_meta::{Key, MetaStore, Record, Versioned};

use crate::catalog::LogicStore;
use crate::cron::Cron;
use crate::deploy::conn_err;

/// The worker job kind of one run.
pub const JOB_KIND: &str = "logic_schedule";

/// `logic/schedules/<tenant>/<module>` in the tenant's home DC tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleIndex {
    /// The tenant.
    pub tenant: TenantId,
    /// The schedule's module.
    pub module_id: u64,
    /// The shard schema its routine is in.
    pub schema: String,
    /// When it next fires, Unix ms (a copy of `next_fire_at`; the catalog
    /// decides).
    pub next_fire_ms: u64,
}

impl Versioned for ScheduleIndex {
    const V: u32 = 1;
}

/// The index key of a schedule.
///
/// # Errors
/// A bad key.
pub fn index_key(store: &dyn MetaStore, tenant: TenantId, module_id: u64) -> Result<Key> {
    store
        .tier()
        .key(&format!("logic/schedules/{tenant}/{module_id}"))
}

/// The index prefix.
///
/// # Errors
/// A bad key.
pub fn index_prefix(store: &dyn MetaStore) -> Result<Key> {
    store.tier().key("logic/schedules/")
}

/// Writes a schedule's index entry.
///
/// # Errors
/// The store's.
pub async fn put_index(store: &dyn MetaStore, e: &ScheduleIndex) -> Result<()> {
    let key = index_key(store, e.tenant, e.module_id)?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key,
                value: Record::new(e.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Every index entry.
///
/// # Errors
/// The store's.
pub async fn list_index(store: &dyn MetaStore) -> Result<Vec<ScheduleIndex>> {
    let prefix = index_prefix(store)?;
    let kvs = store
        .list(
            &prefix,
            sprig_meta::ListOpts {
                limit: 5000,
                ..sprig_meta::ListOpts::default()
            },
        )
        .await?;
    Ok(kvs
        .kvs
        .iter()
        .filter_map(|kv| Record::<ScheduleIndex>::decode(&kv.value).ok())
        .map(|r| r.body)
        .collect())
}

/// What a tick did to one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tick {
    /// Not due, paused, or another tick got there.
    Nothing,
    /// A run is live and the fire time hasn't come.
    Running,
    /// Fire: submit job `job`.
    Fire(JobId),
    /// The fire time passed while a run was live (`skip`), or was queued.
    Skipped,
    /// Late beyond `logic.schedule_misfire_seconds`.
    Misfired,
}

fn next_ms(cron: &Cron, after_ms: u64) -> u64 {
    let after = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        i64::try_from(after_ms).unwrap_or(0),
    )
    .unwrap_or_default();
    cron.next_after(after).map_or(u64::MAX / 2, |t| {
        u64::try_from(t.timestamp_millis()).unwrap_or(0)
    })
}

fn job_bytes(hex: &str) -> Option<[u8; 16]> {
    let v = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    v.try_into().ok()
}

/// One tick of one schedule (see the module docs). `alive(job)` says
/// whether a job id names a job that isn't finished. Returns what to do
/// and the schedule's next fire time afterwards.
///
/// # Errors
/// Catalog errors; 39873 for a stored cron text that no longer parses.
pub async fn tick(
    c: &mut Conn,
    store: &LogicStore,
    module_id: u64,
    now_ms: u64,
    misfire: Duration,
    alive: impl Fn(&str) -> bool,
) -> Result<(Tick, Option<u64>)> {
    let Some(mut s) = store.schedule(c, module_id).await? else {
        return Ok((Tick::Nothing, None));
    };
    if s.paused {
        return Ok((Tick::Nothing, Some(s.next_fire_ms)));
    }
    let cron = Cron::parse(&s.cron, &s.time_zone)?;
    if let Some(j) = s.running_job_id.clone() {
        if alive(&j) {
            if s.next_fire_ms > now_ms {
                return Ok((Tick::Running, Some(s.next_fire_ms)));
            }
            let next = next_ms(&cron, now_ms);
            let queue = s.overlap == "queue_one";
            store
                .pass(c, module_id, s.next_fire_ms, next, "skipped", queue)
                .await?;
            return Ok((Tick::Skipped, Some(next)));
        }
        // The run's job is gone (a controller died between the
        // compare-and-set and the submit, or the job failed for good).
        if let Some(b) = job_bytes(&j) {
            store.run_ended(c, module_id, &b, "error").await?;
        }
        s.running_job_id = None;
    }
    if s.queued {
        let job = JobId::new_v7();
        if store.fire_queued(c, module_id, &job.to_bytes()).await? {
            return Ok((Tick::Fire(job), Some(s.next_fire_ms)));
        }
    }
    if s.next_fire_ms > now_ms {
        return Ok((Tick::Nothing, Some(s.next_fire_ms)));
    }
    let next = next_ms(&cron, now_ms);
    if now_ms - s.next_fire_ms > u64::try_from(misfire.as_millis()).unwrap_or(u64::MAX) {
        store
            .pass(c, module_id, s.next_fire_ms, next, "misfired", false)
            .await?;
        return Ok((Tick::Misfired, Some(next)));
    }
    let job = JobId::new_v7();
    if store
        .fire(c, module_id, s.next_fire_ms, next, &job.to_bytes())
        .await?
    {
        Ok((Tick::Fire(job), Some(next)))
    } else {
        Ok((Tick::Nothing, Some(next)))
    }
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEnd {
    /// `ok`, `error`, `budget`, or `superseded` (not this job's run).
    pub outcome: String,
    /// The error, if any.
    pub error: Option<String>,
    /// A queued run waits.
    pub queued: bool,
}

/// Runs schedule `module_id`'s run `job` (see the module docs): `catalog`
/// is the tenant's catalog, `shard` a `sprig_ctl` connection to the
/// cluster holding `schema`.
///
/// # Errors
/// Catalog errors (the run's own error is in [`RunEnd`]).
pub async fn run(
    catalog: &mut Conn,
    store: &LogicStore,
    shard: &mut Conn,
    schema: &str,
    module_id: u64,
    job: JobId,
    budget: Duration,
) -> Result<RunEnd> {
    let id = job.to_bytes();
    if !store.is_running(catalog, module_id, &id).await? {
        return Ok(RunEnd {
            outcome: "superseded".into(),
            error: None,
            queued: false,
        });
    }
    let m = store.module_by_id(catalog, module_id).await?;
    let version = m.and_then(|m| m.active_version).unwrap_or(0);
    let started = std::time::Instant::now();
    let secs = budget.as_secs_f64().max(0.001);
    let sql = format!(
        "SET STATEMENT max_statement_time = {secs} FOR CALL {}.`__sprig_l{module_id}_v{version}`()",
        quote(schema)
    );
    let r: std::result::Result<(), SprigError> = async {
        let mut q = shard.query_iter(sql).await.map_err(conn_err)?;
        while !q.is_empty() {
            let _: Vec<mysql_async::Row> = q.collect().await.map_err(conn_err)?;
        }
        Ok(())
    }
    .await;
    let (outcome, error) = match &r {
        Ok(()) => ("ok", None),
        Err(e)
            if e.message.contains("max_statement_time")
                || matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1969, .. }) =>
        {
            ("budget", Some(e.to_string()))
        }
        Err(e) => ("error", Some(e.to_string())),
    };
    store.run_ended(catalog, module_id, &id, outcome).await?;
    let queued = store
        .schedule(catalog, module_id)
        .await?
        .is_some_and(|s| s.queued);
    let msg = match &error {
        None => format!(
            "run {job} of version {version} ok in {} ms",
            started.elapsed().as_millis()
        ),
        Some(e) => format!("run {job} of version {version} failed: {e}"),
    };
    let _ = store
        .log(
            catalog,
            module_id,
            Some(version),
            if error.is_some() { "error" } else { "info" },
            "schedule",
            &msg,
        )
        .await;
    Ok(RunEnd {
        outcome: outcome.to_owned(),
        error,
        queued,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_ids_round_trip() {
        let j = JobId::new_v7();
        let hex = j.to_string();
        assert_eq!(job_bytes(&hex), Some(j.to_bytes()));
        assert_eq!(job_bytes("zz"), None);
    }

    #[test]
    fn next_fire_times() {
        let c = Cron::parse("*/5 * * * *", "UTC").unwrap_or_else(|_| unreachable!());
        // 2026-11-09T10:02:00Z → 10:05.
        assert_eq!(next_ms(&c, 1_794_218_520_000), 1_794_218_700_000);
    }
}
