//! `node_rebuild` (chapter 25, "Recovery after an outage", step 3): what
//! the worker that runs the job and the agents that do the work share.
//!
//! ```text
//! worker (node_rebuild job, class recovery):
//!   ticket rebuilds/<job> = {cluster, target, source, sha256(token), rate, expiry}
//!   POST target agent /internal/v1/rebuild {job, token}; poll its status
//! target agent:
//!   GET source agent /internal/v1/backup?job=&token=   (mariadb-backup
//!     --stream=mbstream --slave-info from a healthy replica, never the writer,
//!     at most recovery.rebuild_max_mb_per_second)
//!   | mbstream -x into the staging directory; mariadb-backup --prepare
//!   stop mysqld; the old data directory set aside; --move-back; start mysqld
//!   (it boots read_only); report the backup's gtid_slave_pos
//! worker:
//!   STOP SLAVE; RESET SLAVE ALL; SET GLOBAL gtid_slave_pos = <backup's>;
//!   CHANGE MASTER TO the writer, MASTER_USE_GTID = slave_pos; START SLAVE
//! ```
//!
//! The backup stream is tenant data, so a source agent serves it only to a
//! request that presents the token whose SHA-256 the ticket holds, for the
//! ticket's source node, before the ticket expires. The token itself never
//! goes to etcd.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use sprig_meta::{Key, Tier, Versioned};

use super::primary_replica::ReplSource;
use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, literal};

/// The job kind.
pub const KIND: &str = "node_rebuild";
/// The agent's route that starts or reports a rebuild.
pub const REBUILD_ROUTE: &str = "/internal/v1/rebuild";
/// The agent's route that cancels a rebuild.
pub const CANCEL_ROUTE: &str = "/internal/v1/rebuild/cancel";
/// The agent's route that streams a backup of its node.
pub const BACKUP_ROUTE: &str = "/internal/v1/backup";

/// `rebuilds/<job_id>` in `tier`.
///
/// # Errors
/// Internal for a job id that isn't a key segment.
pub fn ticket_key(tier: &Tier, job_id: &str) -> Result<Key> {
    tier.key(&format!("rebuilds/{job_id}"))
}

/// What the worker tells both agents (`rebuilds/<job_id>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildTicket {
    /// The job.
    pub job_id: String,
    /// The job's attempt: a new owner writes a new ticket and token.
    pub attempt: u32,
    /// The cluster.
    pub cluster: String,
    /// The node rebuilt.
    pub target_node: String,
    /// The replica the backup is streamed from.
    pub source_node: String,
    /// Where the source agent's internal service listens.
    pub source_url: String,
    /// SHA-256 of the stream token, hex.
    pub token_sha256: String,
    /// The stream's rate cap, MB per second (`recovery.rebuild_max_mb_per_second`).
    pub rate_mb_per_second: u64,
    /// Keep the old data directory beside the new one.
    pub keep_old: bool,
    /// The ticket's end, Unix ms.
    pub expires_unix_ms: u64,
}

impl Versioned for RebuildTicket {
    const V: u32 = 1;
}

impl RebuildTicket {
    /// Whether `token` is this ticket's, before `now_ms`.
    #[must_use]
    pub fn admits(&self, token: &str, now_ms: u64) -> bool {
        now_ms < self.expires_unix_ms && constant_eq(&sha256_hex(token), &self.token_sha256)
    }
}

/// A new random stream token (256 bits, hex).
///
/// # Errors
/// Internal when the system's random source fails.
pub fn new_token() -> Result<String> {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b)
        .map_err(|e| sprig_common::SprigError::internal(format!("random token: {e}")))?;
    Ok(hex(&b))
}

/// SHA-256 of `s`, hex.
#[must_use]
pub fn sha256_hex(s: &str) -> String {
    hex(&Sha256::digest(s.as_bytes()))
}

fn hex(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(out, "{x:02x}");
    }
    out
}

fn constant_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// `POST /internal/v1/rebuild`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartRebuild {
    /// The job.
    pub job_id: String,
    /// The stream token (its hash is in the ticket).
    pub token: String,
}

/// Where a rebuild is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebuildPhase {
    /// Accepted, not started.
    Queued,
    /// Streaming the backup into the staging directory (mysqld still runs, fenced).
    Streaming,
    /// `mariadb-backup --prepare`.
    Preparing,
    /// Stopping mysqld.
    Stopping,
    /// Setting the old data directory aside and moving the backup in.
    Restoring,
    /// Starting mysqld and waiting for it to answer.
    Starting,
    /// mysqld runs on the restored data, `read_only`; the worker attaches it.
    Started,
    /// Stopped with an error (see `error`).
    Failed,
    /// Cancelled before the data directory was touched.
    Cancelled,
}

impl RebuildPhase {
    /// Whether the agent is done with it.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, Self::Started | Self::Failed | Self::Cancelled)
    }
}

/// `GET /internal/v1/rebuild?job=` (and the answer to a start).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildStatus {
    /// The job.
    pub job_id: String,
    /// Where it is.
    pub phase: RebuildPhase,
    /// Bytes of the backup stream received.
    #[serde(default)]
    pub bytes: u64,
    /// Since the agent accepted it, ms.
    #[serde(default)]
    pub elapsed_ms: u64,
    /// How long mysqld was down (stop to answering), ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downtime_ms: Option<u64>,
    /// The backup's `gtid_slave_pos` (its source replica's applied position).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gtid: Option<String>,
    /// Where the old data directory was set aside, when kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_datadir: Option<String>,
    /// Why it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The backup's start position from `mariadb_backup_slave_info`
/// (`SET GLOBAL gtid_slave_pos = '1-5-7';`), else from
/// `mariadb_backup_binlog_info` (`<file>\t<pos>\t<gtid>`).
#[must_use]
pub fn backup_gtid(slave_info: Option<&str>, binlog_info: Option<&str>) -> Option<String> {
    if let Some(s) = slave_info
        && let Some(i) = s.find("gtid_slave_pos")
    {
        let rest = &s[i..];
        if let (Some(a), Some(b)) = (rest.find('\''), rest.rfind('\''))
            && b > a
        {
            return Some(rest[a + 1..b].to_owned());
        }
    }
    let line = binlog_info?.lines().next()?;
    line.split('\t').nth(2).map(|g| g.trim().to_owned())
}

/// The worker's last step: the restored node replicates from the writer,
/// starting after the backup's position; `read_only` stays on.
///
/// # Errors
/// A statement failed.
pub async fn attach(ep: &NodeEndpoint, to: &ReplSource, gtid: &str) -> Result<(), ProbeError> {
    let mut s = ProbeSession::connect(ep, std::time::Duration::from_secs(10)).await?;
    s.exec("SET GLOBAL read_only = ON").await?;
    s.exec("STOP SLAVE").await?;
    s.exec("RESET SLAVE ALL").await?;
    s.exec(&format!("SET GLOBAL gtid_slave_pos = {}", literal(gtid)))
        .await?;
    s.exec(&format!(
        "CHANGE MASTER TO MASTER_HOST = {}, MASTER_PORT = {}, MASTER_USER = {}, \
         MASTER_PASSWORD = {}, MASTER_USE_GTID = slave_pos",
        literal(&to.host),
        to.port,
        literal(&to.user),
        literal(&to.password)
    ))
    .await?;
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = OFF")
        .await?;
    s.exec("SET GLOBAL rpl_semi_sync_slave_enabled = ON")
        .await?;
    s.exec("START SLAVE").await?;
    s.close().await;
    Ok(())
}

/// What the worker reads to judge an attached node: both threads, the
/// last errors, the applied position.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Attached {
    /// `Slave_IO_Running`.
    pub io: String,
    /// `Slave_SQL_Running`.
    pub sql: String,
    /// `Last_IO_Error` or `Last_SQL_Error`, when either is set.
    pub error: Option<String>,
    /// `@@gtid_slave_pos`.
    pub applied: String,
}

/// Reads [`Attached`].
///
/// # Errors
/// The node couldn't be reached.
pub async fn attached(ep: &NodeEndpoint) -> Result<Attached, ProbeError> {
    let mut s = ProbeSession::connect(ep, std::time::Duration::from_secs(5)).await?;
    let rows = s.rows("SHOW SLAVE STATUS").await?;
    let applied = s
        .scalar("SELECT @@gtid_slave_pos")
        .await?
        .unwrap_or_default();
    s.close().await;
    let Some(r) = rows.first() else {
        return Ok(Attached {
            applied,
            ..Attached::default()
        });
    };
    let get = |n: &str| {
        r.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(n))
            .and_then(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let error = [get("Last_IO_Error"), get("Last_SQL_Error")]
        .into_iter()
        .find(|e| !e.is_empty());
    Ok(Attached {
        io: get("Slave_IO_Running"),
        sql: get("Slave_SQL_Running"),
        error,
        applied,
    })
}

/// A rebuild spec as the job record carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRebuildSpec {
    /// The cluster.
    pub cluster: ClusterId,
    /// The node to rebuild.
    pub node: String,
    /// The replica to stream from; unset: the healthiest replica that has
    /// an agent, never the writer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Refuse to discard a diverged node's data before its errant extract
    /// is verified (chapter 25 step 2.5). `false` (`--no-extract`) keeps the
    /// old data directory instead.
    #[serde(default = "yes")]
    pub require_extract: bool,
}

const fn yes() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backup_position_comes_from_the_slave_info_first() {
        let slave = "SET GLOBAL gtid_slave_pos = '1-5-7,2-9-3';\nCHANGE MASTER TO master_use_gtid = slave_pos;\n";
        let binlog = "mysql-bin.000002\t1411\t1-5-7\n";
        assert_eq!(
            backup_gtid(Some(slave), Some(binlog)).as_deref(),
            Some("1-5-7,2-9-3")
        );
        assert_eq!(backup_gtid(None, Some(binlog)).as_deref(), Some("1-5-7"));
        assert_eq!(backup_gtid(None, None), None);
        assert_eq!(backup_gtid(Some("garbage"), None), None);
    }

    #[test]
    fn a_ticket_admits_only_its_token_before_it_expires() {
        let token = new_token().expect("token");
        assert_eq!(token.len(), 64);
        let t = RebuildTicket {
            job_id: "j".into(),
            attempt: 1,
            cluster: "fra1-c1".into(),
            target_node: "db1".into(),
            source_node: "db2".into(),
            source_url: "http://127.0.0.1:1".into(),
            token_sha256: sha256_hex(&token),
            rate_mb_per_second: 200,
            keep_old: false,
            expires_unix_ms: 1_000,
        };
        assert!(t.admits(&token, 999));
        assert!(!t.admits(&token, 1_000));
        assert!(!t.admits("other", 10));
        assert_ne!(new_token().expect("token"), token);
    }

    #[test]
    fn specs_require_the_extract_by_default() {
        let s: NodeRebuildSpec =
            serde_json::from_str(r#"{"cluster":"fra1-c1","node":"db1"}"#).expect("decode");
        assert!(s.require_extract);
        assert_eq!(s.source, None);
    }
}
