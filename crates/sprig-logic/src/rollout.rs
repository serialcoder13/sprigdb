//! Staged rollout of functions and procedures (chapter 41, "Versions,
//! rollout and rollback"; decision RC10; R7 follow-ups).
//!
//! ```text
//! CREATE OR REPLACE FUNCTION f ... WITH (SPRIG_ROLLOUT = 10 [, SPRIG_ROLLOUT_BY = 'user' | 'tenant']) AS ...
//!   → version n+1 is the candidate for 10% of callers; the active version
//!     answers the rest
//! ALTER FUNCTION f ROLLOUT TO 50 [PERCENT] [BY USER | TENANT]   another stage
//! ALTER FUNCTION f ROLLOUT PROMOTE        (or TO 100) the candidate becomes active
//! ALTER FUNCTION f ROLLOUT ABORT          (or TO 0)   the active version alone again
//! ```
//!
//! **Who gets the candidate.** The function's wrapper procedure (the one
//! every caller reaches, whatever the protocol) holds both versions'
//! bodies and picks one per call:
//!
//! ```text
//! BY USER (default): CRC32(CONCAT(<the caller's backend account>, ':', module)) % 100 < pct
//! BY TENANT:         CRC32(CONCAT('t<tenant>', ':', module)) % 100 < pct, decided at deploy
//! ```
//!
//! So a caller keeps the version it got while the stage lasts, and a
//! larger stage keeps everyone the smaller one had. The gateway computes
//! the same pick ([`picks_candidate`]) to count each call against the
//! version that ran (engine-tier calls name their version anyway).
//!
//! **Numbers and the automatic rollback.** Gateways count calls, errors
//! and a latency histogram per version ([`Stats`]) and add them to
//! `logic_version_stats` every few seconds. After adding, a gateway
//! compares the candidate with the active version ([`verdict`]): with at
//! least [`MIN_CALLS`] candidate calls, an error rate above twice the
//! active one's and above 1%, or (both with [`MIN_CALLS`]) a p95 above
//! twice the active one's, aborts the rollout as `ROLLOUT ABORT` would,
//! and says why in `logic_log`. Counting starts again for both versions
//! when a rollout starts.
//!
//! As built there is no soak timer: each stage is set by hand (or by
//! `sprigctl logic deploy --rollout`), and the automatic rollback is the
//! guard while it runs.

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::Serialize;
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{EventDef, Function, Param, Ty};

/// Calls a version needs before its numbers decide anything.
pub const MIN_CALLS: u64 = 20;

/// Upper bounds of the latency histogram's buckets, in milliseconds; the
/// last bucket holds everything slower.
pub const BUCKETS_MS: [u64; 11] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000];

/// `logic_rollouts`: one row per module with a candidate (migration 5).
pub const DDL_ROLLOUTS: &str = "CREATE TABLE IF NOT EXISTS logic_rollouts (
  module_id         BIGINT UNSIGNED NOT NULL,
  active_version    BIGINT UNSIGNED NOT NULL,
  candidate_version BIGINT UNSIGNED NOT NULL,
  pct               TINYINT UNSIGNED NOT NULL,
  by_key            ENUM('user','tenant') NOT NULL DEFAULT 'user',
  started_at        DATETIME(6)     NOT NULL,
  changed_at        DATETIME(6)     NOT NULL,
  PRIMARY KEY (module_id)
) ENGINE=InnoDB";

/// `logic_version_stats`: calls, errors and latency per version, as
/// gateways add them (migration 5).
pub const DDL_STATS: &str = "CREATE TABLE IF NOT EXISTS logic_version_stats (
  module_id BIGINT UNSIGNED NOT NULL,
  version   BIGINT UNSIGNED NOT NULL,
  calls     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  errors    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  total_us  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  hist      VARCHAR(512)    NOT NULL DEFAULT '',
  since     DATETIME(6)     NOT NULL,
  updated   DATETIME(6)     NOT NULL,
  PRIMARY KEY (module_id, version)
) ENGINE=InnoDB";

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSyntax, m)
}

fn map_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the logic catalog is unreachable",
    )
    .with_source(e)
}

/// A tenant whose catalog has no rollout tables yet (they come with its
/// first deploy after this version) has no rollouts.
fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

/// What a caller's pick is keyed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RolloutBy {
    /// The caller (its backend account): each user keeps one version.
    #[default]
    User,
    /// The tenant: all of a tenant's calls get the same version, for code
    /// deployed to many tenants at once.
    Tenant,
}

impl RolloutBy {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RolloutBy::User => "user",
            RolloutBy::Tenant => "tenant",
        }
    }

    /// Reads `user` or `tenant`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "user" | "caller" => Some(RolloutBy::User),
            "tenant" => Some(RolloutBy::Tenant),
            _ => None,
        }
    }
}

/// A stage: the candidate's share and the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RolloutSpec {
    /// 1 to 99.
    pub pct: u8,
    /// What the pick is keyed on.
    pub by: RolloutBy,
}

/// `ALTER {FUNCTION | PROCEDURE} f ROLLOUT ...`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutOp {
    /// `TO n [PERCENT] [BY ...]`, 1 to 99 (the key stays when not given).
    To(u8, Option<RolloutBy>),
    /// `PROMOTE` or `TO 100`.
    Promote,
    /// `ABORT` or `TO 0`.
    Abort,
}

/// Parses the words after `ROLLOUT` (lower case).
///
/// # Errors
/// 39861 for anything else.
pub fn parse_op(rest: &[String]) -> Result<RolloutOp> {
    let usage = || {
        syntax(
            "expected ALTER FUNCTION f ROLLOUT {TO n [PERCENT] [BY USER | TENANT] | PROMOTE | ABORT}",
        )
    };
    match rest {
        [p] if p == "promote" => Ok(RolloutOp::Promote),
        [a] if a == "abort" => Ok(RolloutOp::Abort),
        [t, n, more @ ..] if t == "to" => {
            let n: u32 = n
                .trim_end_matches('%')
                .parse()
                .map_err(|_| syntax(format!("ROLLOUT TO takes a percentage, not {n}")))?;
            let more: Vec<&str> = more
                .iter()
                .map(String::as_str)
                .filter(|w| *w != "percent")
                .collect();
            let by = match more.as_slice() {
                [] => None,
                ["by", k] => Some(RolloutBy::parse(k).ok_or_else(usage)?),
                _ => return Err(usage()),
            };
            match n {
                0 => Ok(RolloutOp::Abort),
                100 => Ok(RolloutOp::Promote),
                1..=99 => Ok(RolloutOp::To(u8::try_from(n).unwrap_or(99), by)),
                _ => Err(syntax("a rollout stage is 0 to 100 percent")),
            }
        }
        _ => Err(usage()),
    }
}

/// The bucket, 0 to 99, of `key` for `module`: MariaDB's
/// `CRC32(CONCAT(key, ':', module)) % 100`.
#[must_use]
pub fn bucket(key: &str, module_id: u64) -> u32 {
    crc32fast::hash(format!("{key}:{module_id}").as_bytes()) % 100
}

/// Whether a call by `account` (the backend account, `t<tenant>_u<user>`)
/// of tenant `tenant` gets the candidate.
#[must_use]
pub fn picks_candidate(spec: RolloutSpec, account: &str, tenant: u64, module_id: u64) -> bool {
    let key = match spec.by {
        RolloutBy::User => account.to_owned(),
        RolloutBy::Tenant => format!("t{tenant}"),
    };
    bucket(&key, module_id) < u32::from(spec.pct)
}

/// The SQL condition the wrapper uses for a `BY USER` stage.
#[must_use]
pub fn user_condition(module_id: u64, pct: u8) -> String {
    format!("CRC32(CONCAT(SUBSTRING_INDEX(USER(), '@', 1), ':{module_id}')) % 100 < {pct}")
}

fn ty_of(name: &str) -> Ty {
    match name {
        "i64" => Ty::Int,
        "f64" => Ty::Float,
        "bool" => Ty::Bool,
        "json" => Ty::Json,
        "null" => Ty::Null,
        _ => Ty::Str,
    }
}

/// A version's wrapper signature (parameters and events) from its stored
/// signature, as the wrapper generator needs it.
#[must_use]
pub fn function_of(name: &str, signature: &Value) -> Function {
    Function {
        name: name.to_owned(),
        params: signature["params"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| Param {
                name: p["name"].as_str().unwrap_or_default().to_owned(),
                ty: ty_of(p["type"].as_str().unwrap_or("")),
                min: None,
                max: None,
            })
            .collect(),
        events: signature["events"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|e| EventDef {
                name: e["name"].as_str().unwrap_or("output").to_owned(),
                fields: e["fields"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|f| {
                        (
                            f["name"].as_str().unwrap_or_default().to_owned(),
                            ty_of(f["type"].as_str().unwrap_or("")),
                        )
                    })
                    .collect(),
            })
            .collect(),
        locals: Vec::new(),
        body: Vec::new(),
    }
}

/// Whether two versions can share one wrapper: the same parameters, by
/// name and type, in the same order.
#[must_use]
pub fn same_params(a: &Function, b: &Function) -> bool {
    a.params.len() == b.params.len()
        && a.params
            .iter()
            .zip(&b.params)
            .all(|(x, y)| x.name == y.name && x.ty == y.ty)
}

/// A module's rollout, as `logic_rollouts` holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rollout {
    /// The module.
    pub module_id: u64,
    /// The version most callers get.
    pub active_version: u64,
    /// The version on trial.
    pub candidate_version: u64,
    /// The candidate's share.
    pub pct: u8,
    /// What the pick is keyed on.
    pub by: RolloutBy,
    /// When the rollout started (UTC).
    pub started_at: String,
}

impl Rollout {
    /// The stage.
    #[must_use]
    pub fn spec(&self) -> RolloutSpec {
        RolloutSpec {
            pct: self.pct,
            by: self.by,
        }
    }
}

fn q(schema: &str) -> String {
    sprig_common::names::quote(schema)
}

/// Records a rollout's stage (a new candidate resets both versions'
/// numbers), and the book's `candidate_version` and `candidate_pct`.
///
/// # Errors
/// 39070 or a backend error.
pub async fn put(
    c: &mut Conn,
    schema: &str,
    module_id: u64,
    active: u64,
    candidate: u64,
    spec: RolloutSpec,
) -> Result<()> {
    let s = q(schema);
    let prev: Option<(u64, u64)> = c
        .exec_first(
            format!(
                "SELECT active_version, candidate_version FROM {s}.logic_rollouts WHERE module_id = ?"
            ),
            (module_id,),
        )
        .await
        .map_err(map_err)?;
    let fresh = prev != Some((active, candidate));
    if fresh {
        c.exec_drop(
            format!("DELETE FROM {s}.logic_version_stats WHERE module_id = ?"),
            (module_id,),
        )
        .await
        .map_err(map_err)?;
    }
    c.exec_drop(
        format!(
            "INSERT INTO {s}.logic_rollouts (module_id, active_version, candidate_version, pct, by_key, \
             started_at, changed_at) VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)) \
             ON DUPLICATE KEY UPDATE pct = VALUES(pct), by_key = VALUES(by_key), \
             changed_at = VALUES(changed_at), \
             started_at = IF(active_version = VALUES(active_version) AND \
               candidate_version = VALUES(candidate_version), started_at, VALUES(started_at)), \
             active_version = VALUES(active_version), candidate_version = VALUES(candidate_version)"
        ),
        (module_id, active, candidate, spec.pct, spec.by.as_str()),
    )
    .await
    .map_err(map_err)?;
    c.exec_drop(
        format!(
            "UPDATE {s}.logic_modules SET candidate_version = ?, candidate_pct = ? WHERE module_id = ?"
        ),
        (candidate, spec.pct, module_id),
    )
    .await
    .map_err(map_err)?;
    c.exec_drop(
        format!(
            "UPDATE {s}.logic_versions SET state = 'rolling_out' WHERE module_id = ? AND version = ?"
        ),
        (module_id, candidate),
    )
    .await
    .map_err(map_err)
}

/// Ends a module's rollout, if it has one (the candidate, unless it was
/// just activated, goes back to `superseded`).
///
/// # Errors
/// 39070 or a backend error.
pub async fn clear(c: &mut Conn, schema: &str, module_id: u64) -> Result<Option<Rollout>> {
    let prev = get(c, schema, module_id).await?;
    let s = q(schema);
    if prev.is_some() {
        c.exec_drop(
            format!("DELETE FROM {s}.logic_rollouts WHERE module_id = ?"),
            (module_id,),
        )
        .await
        .map_err(map_err)?;
    }
    c.exec_drop(
        format!(
            "UPDATE {s}.logic_modules SET candidate_version = NULL, candidate_pct = 0 WHERE module_id = ?"
        ),
        (module_id,),
    )
    .await
    .map_err(map_err)?;
    c.exec_drop(
        format!(
            "UPDATE {s}.logic_versions SET state = 'superseded' WHERE module_id = ? AND state = 'rolling_out'"
        ),
        (module_id,),
    )
    .await
    .map_err(map_err)?;
    Ok(prev)
}

type RolloutTuple = (u64, u64, u64, u8, String, String);

fn rollout_of(t: RolloutTuple) -> Rollout {
    Rollout {
        module_id: t.0,
        active_version: t.1,
        candidate_version: t.2,
        pct: t.3,
        by: RolloutBy::parse(&t.4).unwrap_or_default(),
        started_at: t.5,
    }
}

const ROLLOUT_COLS: &str = "module_id, active_version, candidate_version, pct, CAST(by_key AS CHAR), \
     DATE_FORMAT(started_at, '%Y-%m-%dT%H:%i:%s.%fZ')";

/// A module's rollout.
///
/// # Errors
/// 39070 or a backend error.
pub async fn get(c: &mut Conn, schema: &str, module_id: u64) -> Result<Option<Rollout>> {
    let r: std::result::Result<Option<RolloutTuple>, _> = c
        .exec_first(
            format!(
                "SELECT {ROLLOUT_COLS} FROM {}.logic_rollouts WHERE module_id = ?",
                q(schema)
            ),
            (module_id,),
        )
        .await;
    match r {
        Err(e) if missing_table(&e) => Ok(None),
        r => Ok(r.map_err(map_err)?.map(rollout_of)),
    }
}

/// Every rollout of the tenant, with each module's database and name,
/// for the gateway's cache.
///
/// # Errors
/// 39070 or a backend error.
pub async fn all(c: &mut Conn, schema: &str) -> Result<Vec<(u32, String, Rollout)>> {
    type T = (u32, String, u64, u64, u64, u8, String, String);
    let s = q(schema);
    let r: std::result::Result<Vec<T>, _> = c
        .query(format!(
            "SELECT m.db_id, m.name, r.module_id, r.active_version, r.candidate_version, r.pct, \
             CAST(r.by_key AS CHAR), DATE_FORMAT(r.started_at, '%Y-%m-%dT%H:%i:%s.%fZ') \
             FROM {s}.logic_rollouts r JOIN {s}.logic_modules m ON m.module_id = r.module_id \
             WHERE m.state = 'active' AND m.kind IN ('function', 'procedure')"
        ))
        .await;
    match r {
        Err(e) if missing_table(&e) => Ok(Vec::new()),
        r => Ok(r
            .map_err(map_err)?
            .into_iter()
            .map(|(db, name, m, a, cand, p, by, at)| {
                (db, name, rollout_of((m, a, cand, p, by, at)))
            })
            .collect()),
    }
}

/// Calls of one version counted by one gateway since its last flush.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Acc {
    /// Calls.
    pub calls: u64,
    /// Calls that failed.
    pub errors: u64,
    /// Their total duration, microseconds.
    pub total_us: u64,
    /// Calls per latency bucket ([`BUCKETS_MS`], then slower).
    pub hist: [u64; 12],
}

impl Acc {
    /// Counts one call.
    pub fn add(&mut self, elapsed: std::time::Duration, failed: bool) {
        self.calls += 1;
        if failed {
            self.errors += 1;
        }
        self.total_us += u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let i = BUCKETS_MS.iter().position(|b| ms < *b).unwrap_or(11);
        self.hist[i] += 1;
    }
}

fn hist_text(h: &[u64; 12]) -> String {
    h.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

fn hist_parse(s: &str) -> [u64; 12] {
    let mut h = [0u64; 12];
    for (i, x) in s.split(',').take(12).enumerate() {
        h[i] = x.trim().parse().unwrap_or(0);
    }
    h
}

/// Adds one gateway's counts to `logic_version_stats`.
///
/// # Errors
/// 39070 or a backend error.
pub async fn flush(c: &mut Conn, schema: &str, rows: &[((u64, u64), Acc)]) -> Result<()> {
    let s = q(schema);
    for ((module, version), a) in rows {
        let prev: Option<String> = c
            .exec_first(
                format!(
                    "SELECT hist FROM {s}.logic_version_stats WHERE module_id = ? AND version = ? FOR UPDATE"
                ),
                (module, version),
            )
            .await
            .map_err(map_err)?;
        let mut h = prev.as_deref().map(hist_parse).unwrap_or_default();
        for (x, y) in h.iter_mut().zip(a.hist) {
            *x += y;
        }
        c.exec_drop(
            format!(
                "INSERT INTO {s}.logic_version_stats (module_id, version, calls, errors, total_us, hist, since, updated) \
                 VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE \
                 calls = calls + VALUES(calls), errors = errors + VALUES(errors), \
                 total_us = total_us + VALUES(total_us), hist = VALUES(hist), updated = VALUES(updated)"
            ),
            (module, version, a.calls, a.errors, a.total_us, hist_text(&h)),
        )
        .await
        .map_err(map_err)?;
    }
    Ok(())
}

/// One version's numbers.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionStats {
    /// The version.
    pub version: u64,
    /// Calls.
    pub calls: u64,
    /// Failed calls.
    pub errors: u64,
    /// `errors / calls`.
    pub error_rate: f64,
    /// Mean duration, milliseconds.
    pub mean_ms: f64,
    /// The 95th percentile, as the upper bound of its bucket in
    /// milliseconds (`None` past the last bound).
    pub p95_ms: Option<u64>,
    /// Calls per bucket: the bounds are [`BUCKETS_MS`], then slower.
    pub histogram: Vec<u64>,
    /// Counted since (UTC).
    pub since: String,
}

fn p95(h: &[u64; 12], calls: u64) -> Option<u64> {
    if calls == 0 {
        return None;
    }
    let need = calls.saturating_mul(95).div_ceil(100);
    let mut seen = 0;
    for (i, n) in h.iter().enumerate() {
        seen += n;
        if seen >= need {
            return BUCKETS_MS.get(i).copied();
        }
    }
    None
}

/// The numbers of every version of `module_id` that has any.
///
/// # Errors
/// 39070 or a backend error.
pub async fn stats(c: &mut Conn, schema: &str, module_id: u64) -> Result<Vec<VersionStats>> {
    type T = (u64, u64, u64, u64, String, String);
    let r: std::result::Result<Vec<T>, _> = c
        .exec(
            format!(
                "SELECT version, calls, errors, total_us, hist, DATE_FORMAT(since, '%Y-%m-%dT%H:%i:%s.%fZ') \
                 FROM {}.logic_version_stats WHERE module_id = ? ORDER BY version DESC",
                q(schema)
            ),
            (module_id,),
        )
        .await;
    let rows = match r {
        Err(e) if missing_table(&e) => Vec::new(),
        r => r.map_err(map_err)?,
    };
    Ok(rows
        .into_iter()
        .map(|(version, calls, errors, total_us, hist, since)| {
            let h = hist_parse(&hist);
            #[allow(clippy::cast_precision_loss)]
            let (rate, mean) = if calls == 0 {
                (0.0, 0.0)
            } else {
                (
                    errors as f64 / calls as f64,
                    total_us as f64 / calls as f64 / 1000.0,
                )
            };
            VersionStats {
                version,
                calls,
                errors,
                error_rate: rate,
                mean_ms: mean,
                p95_ms: p95(&h, calls),
                histogram: h.to_vec(),
                since,
            }
        })
        .collect())
}

/// Whether the candidate should be rolled back, and why (see the module
/// docs); `None` while it holds up or has too few calls.
#[must_use]
pub fn verdict(active: Option<&VersionStats>, candidate: &VersionStats) -> Option<String> {
    if candidate.calls < MIN_CALLS {
        return None;
    }
    let a_rate = active
        .filter(|a| a.calls >= MIN_CALLS)
        .map_or(0.0, |a| a.error_rate);
    if candidate.error_rate > 0.01 && candidate.error_rate > 2.0 * a_rate {
        return Some(format!(
            "the candidate's error rate is {:.1}% ({} of {} calls) against {:.1}% for the active version",
            candidate.error_rate * 100.0,
            candidate.errors,
            candidate.calls,
            a_rate * 100.0
        ));
    }
    if let Some(a) = active.filter(|a| a.calls >= MIN_CALLS)
        && let (Some(ap), Some(cp)) = (a.p95_ms, candidate.p95_ms.or(Some(u64::MAX)))
        && cp > 2 * ap
    {
        return Some(format!(
            "the candidate's p95 is {} against {ap} ms for the active version",
            candidate
                .p95_ms
                .map_or_else(|| "over 5000 ms".to_owned(), |p| format!("{p} ms"))
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn w(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn ops() {
        assert_eq!(parse_op(&w("promote")).unwrap(), RolloutOp::Promote);
        assert_eq!(parse_op(&w("abort")).unwrap(), RolloutOp::Abort);
        assert_eq!(
            parse_op(&w("to 25 percent")).unwrap(),
            RolloutOp::To(25, None)
        );
        assert_eq!(
            parse_op(&w("to 5% by tenant")).unwrap(),
            RolloutOp::To(5, Some(RolloutBy::Tenant))
        );
        assert_eq!(parse_op(&w("to 100")).unwrap(), RolloutOp::Promote);
        assert_eq!(parse_op(&w("to 0")).unwrap(), RolloutOp::Abort);
        assert!(parse_op(&w("to 101")).is_err());
        assert!(parse_op(&w("to x")).is_err());
        assert!(parse_op(&w("sideways")).is_err());
        assert!(parse_op(&w("to 5 by row")).is_err());
    }

    #[test]
    fn picks_match_mariadb_and_grow_with_the_stage() {
        // MariaDB: SELECT CRC32('t42_u7:1001') → 1719640002, % 100 → 2.
        assert_eq!(crc32fast::hash(b"t42_u7:1001"), 1_719_640_002);
        assert_eq!(bucket("t42_u7", 1001), 2);
        let users: Vec<String> = (0..1000).map(|u| format!("t42_u{u}")).collect();
        let mut prev = 0;
        for pct in [1u8, 10, 50, 99] {
            let spec = RolloutSpec {
                pct,
                by: RolloutBy::User,
            };
            let n = users
                .iter()
                .filter(|u| picks_candidate(spec, u, 42, 1001))
                .count();
            assert!(n >= prev, "a larger stage keeps the smaller one's callers");
            assert!(
                n.abs_diff(usize::from(pct) * 10) < 60,
                "{pct}%: {n} of 1000"
            );
            prev = n;
        }
        let t = RolloutSpec {
            pct: 50,
            by: RolloutBy::Tenant,
        };
        let a = picks_candidate(t, "t42_u1", 42, 1001);
        assert!(users.iter().all(|u| picks_candidate(t, u, 42, 1001) == a));
        assert_eq!(
            user_condition(1001, 10),
            "CRC32(CONCAT(SUBSTRING_INDEX(USER(), '@', 1), ':1001')) % 100 < 10"
        );
    }

    #[test]
    fn signatures_and_numbers() {
        let sig = serde_json::json!({
            "params": [{"name": "id", "type": "i64"}, {"name": "q", "type": "f64", "sql": "double"}],
            "events": [{"name": "output", "fields": [{"name": "total", "type": "f64"}]}],
        });
        let f = function_of("quote", &sig);
        assert_eq!(f.params[1].ty, Ty::Float);
        assert_eq!(f.events[0].fields[0], ("total".to_owned(), Ty::Float));
        let mut g = f.clone();
        assert!(same_params(&f, &g));
        g.params[0].ty = Ty::Str;
        assert!(!same_params(&f, &g));

        let mut a = Acc::default();
        for i in 0..100u64 {
            a.add(std::time::Duration::from_millis(i % 10), i % 50 == 0);
        }
        assert_eq!((a.calls, a.errors), (100, 2));
        assert_eq!(a.hist.iter().sum::<u64>(), 100);
        assert_eq!(p95(&a.hist, a.calls), Some(10));
        assert_eq!(hist_parse(&hist_text(&a.hist)), a.hist);
        let vs = |calls, errors, p95_ms| VersionStats {
            version: 1,
            calls,
            errors,
            #[allow(clippy::cast_precision_loss)]
            error_rate: if calls == 0 {
                0.0
            } else {
                errors as f64 / calls as f64
            },
            mean_ms: 1.0,
            p95_ms,
            histogram: vec![],
            since: String::new(),
        };
        assert!(
            verdict(Some(&vs(100, 0, Some(5))), &vs(10, 5, Some(5))).is_none(),
            "too few"
        );
        assert!(verdict(Some(&vs(100, 0, Some(5))), &vs(40, 4, Some(5))).is_some());
        assert!(
            verdict(Some(&vs(100, 10, Some(5))), &vs(40, 4, Some(5))).is_none(),
            "as bad as active"
        );
        assert!(
            verdict(None, &vs(40, 1, Some(5))).is_some(),
            "2.5% with no active numbers"
        );
        assert!(
            verdict(Some(&vs(100, 0, Some(5))), &vs(40, 0, Some(20))).is_some(),
            "p95"
        );
        assert!(verdict(Some(&vs(100, 0, Some(10))), &vs(40, 0, Some(20))).is_none());
    }
}
