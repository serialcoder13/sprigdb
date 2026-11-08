//! Primary + replicas, driven by SprigDB (chapter 25 steps 3-10).
//!
//! ```text
//! 3. on each replica whose agent answers: STOP SLAVE IO_THREAD, then
//!    MASTER_GTID_WAIT(Gtid_IO_Pos, 30) so the relay log is applied
//! 4. best = the replica whose gtid_slave_pos is highest in c's domain and contains
//!    every other replica's; if two diverge, stop and alert
//! 5. expected loss: at most 1 s if Rpl_semi_sync_master_status was ON in the last
//!    per-second sample (AFTER_SYNC); otherwise best's last measured lag
//! 6. loss > rpo, or failover.mode manual -> stop, alert, wait for an operator
//! 8. best: STOP SLAVE; RESET SLAVE ALL; rpl_semi_sync_master_enabled = ON;
//!    read_only = OFF
//! 9. others: CHANGE MASTER TO best, MASTER_USE_GTID = slave_pos; START SLAVE
//! ```
//!
//! Steps 1-2 (detection, the drift wait), 7 (the writer lease) and 10 (the
//! cluster record) are the controller's. A returning former primary is
//! reported by [`diverged`] and [`diverged_events`]: the transactions it
//! logged after the position the new primary had applied at promotion.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_binlog::gtid::{Gtid, GtidPos};
use sprig_common::ids::NodeId;

use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow, literal};

/// How long `MASTER_GTID_WAIT` may wait for the relay log (step 3).
pub const RELAY_WAIT_SECONDS: u32 = 30;

/// Statement timeout for failover steps.
pub const STEP_TIMEOUT: Duration = Duration::from_secs(40);

/// What one replica looked like after its relay log was applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaState {
    /// The node.
    pub node: NodeId,
    /// `@@gtid_slave_pos` after the relay log was applied.
    pub slave_pos: String,
    /// `Seconds_Behind_Master` before the IO thread stopped.
    pub lag_seconds: Option<u64>,
    /// Whether the relay log was fully applied within the wait.
    pub drained: bool,
}

impl ReplicaState {
    fn pos(&self) -> GtidPos {
        self.slave_pos.parse().unwrap_or_default()
    }
}

/// The replica to promote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    /// The node.
    pub node: NodeId,
    /// Its applied position: the old primary's transactions it holds.
    pub applied: String,
    /// The expected data loss; `None` means unknown.
    pub expected_loss: Option<Duration>,
    /// Why this one, for the decision log.
    pub why: String,
}

/// The outcome of choosing (step 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    /// Promote this replica (the loss is filled in by [`expected_loss`]).
    Promote(Candidate),
    /// Stop and alert: no replica answered, or two diverge.
    Blocked(String),
}

/// Where replicas replicate from, as they see it.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplSource {
    /// Host as the replicas resolve it.
    pub host: String,
    /// Port.
    pub port: u16,
    /// Replication account.
    pub user: String,
    /// Its password.
    pub password: String,
}

impl std::fmt::Debug for ReplSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplSource")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

async fn session(ep: &NodeEndpoint) -> Result<ProbeSession, ProbeError> {
    ProbeSession::connect(ep, STEP_TIMEOUT).await
}

/// Step 3 on one replica: stop the IO thread, wait for the relay log to
/// be applied, and read the applied position.
///
/// # Errors
/// The replica couldn't be reached or a statement failed.
pub async fn drain_relay(ep: &NodeEndpoint) -> Result<ReplicaState, ProbeError> {
    let mut s = session(ep).await?;
    let before = s.rows("SHOW SLAVE STATUS").await?;
    let lag_seconds = before
        .first()
        .and_then(|r| col(r, "Seconds_Behind_Master"))
        .and_then(|v| v.parse().ok());
    s.exec("STOP SLAVE IO_THREAD").await?;
    let status = s.rows("SHOW SLAVE STATUS").await?;
    let io_pos = status
        .first()
        .and_then(|r| col(r, "Gtid_IO_Pos"))
        .unwrap_or_default()
        .to_owned();
    let drained = if io_pos.is_empty() {
        true
    } else {
        let r = s
            .scalar(&format!(
                "SELECT MASTER_GTID_WAIT({}, {RELAY_WAIT_SECONDS})",
                literal(&io_pos)
            ))
            .await?;
        r.as_deref() == Some("0")
    };
    let slave_pos = s
        .scalar("SELECT @@gtid_slave_pos")
        .await?
        .unwrap_or_default();
    s.close().await;
    Ok(ReplicaState {
        node: ep.node.clone(),
        slave_pos,
        lag_seconds,
        drained,
    })
}

/// Step 4: the replica with the highest position in `domain` that
/// contains every other one's. Equal sequence numbers from different
/// servers mean two replicas diverge.
#[must_use]
pub fn choose(states: &[ReplicaState], domain: u32) -> Choice {
    let seq = |p: &GtidPos| p.get(domain).map_or(0, |g| g.seq);
    let Some(best) = states.iter().max_by(|a, b| {
        seq(&a.pos())
            .cmp(&seq(&b.pos()))
            .then_with(|| b.node.cmp(&a.node))
    }) else {
        return Choice::Blocked("no replica answered".into());
    };
    let bp = best.pos();
    for other in states.iter().filter(|s| s.node != best.node) {
        let op = other.pos();
        let (Some(b), Some(o)) = (bp.get(domain), op.get(domain)) else {
            continue;
        };
        if o.seq == b.seq && o.server != b.server {
            return Choice::Blocked(format!(
                "replicas {} ({o}) and {} ({b}) diverge",
                other.node, best.node
            ));
        }
    }
    let undrained = !best.drained;
    Choice::Promote(Candidate {
        node: best.node.clone(),
        applied: best.slave_pos.clone(),
        expected_loss: None,
        why: format!(
            "highest gtid_slave_pos in domain {domain}: {}{}",
            bp.get(domain).map(ToString::to_string).unwrap_or_default(),
            if undrained {
                " (relay log not fully applied within the wait)"
            } else {
                ""
            }
        ),
    })
}

/// Step 5: at most 1 s when semi-sync was on in the last sample (every
/// acknowledged commit reached a replica); otherwise the best replica's
/// last measured lag; `None` when unknown.
#[must_use]
pub fn expected_loss(semisync_on: bool, lag_seconds: Option<u64>) -> Option<Duration> {
    if semisync_on {
        return Some(Duration::from_secs(1));
    }
    lag_seconds.map(Duration::from_secs)
}

/// Step 6: whether the promotion may proceed automatically.
///
/// # Errors
/// The reason to stop and wait for an operator.
pub fn check_rpo(loss: Option<Duration>, rpo: Duration, manual: bool) -> Result<(), String> {
    if manual {
        return Err("failover.mode is manual".into());
    }
    match loss {
        None => Err("the expected loss is unknown".into()),
        Some(l) if l > rpo => Err(format!(
            "expected loss {} s is over the RPO {} s",
            l.as_secs_f64(),
            rpo.as_secs_f64()
        )),
        Some(_) => Ok(()),
    }
}

/// Whether the node reports `Rpl_semi_sync_master_status = ON` (the
/// per-second sample).
///
/// # Errors
/// The node couldn't be reached.
pub async fn semisync_on(ep: &NodeEndpoint) -> Result<bool, ProbeError> {
    let mut s = session(ep).await?;
    let rows = s
        .rows("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_master_status'")
        .await?;
    s.close().await;
    Ok(rows
        .first()
        .and_then(|r| col(r, "Value"))
        .is_some_and(|v| v.eq_ignore_ascii_case("ON")))
}

/// Step 8: makes `ep` the primary. Call only while its agent holds the
/// writer lease and a DC permit. Returns its applied position.
///
/// # Errors
/// A statement failed; the node may be half promoted (the steps are
/// idempotent, so the caller retries).
pub async fn promote(ep: &NodeEndpoint) -> Result<String, ProbeError> {
    let mut s = session(ep).await?;
    s.exec("STOP SLAVE").await?;
    s.exec("RESET SLAVE ALL").await?;
    s.exec("SET GLOBAL rpl_semi_sync_slave_enabled = OFF").await?;
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = ON").await?;
    s.exec("SET GLOBAL read_only = OFF").await?;
    let pos = s
        .scalar("SELECT @@gtid_slave_pos")
        .await?
        .unwrap_or_default();
    s.close().await;
    Ok(pos)
}

/// Step 9: points a replica at the new primary and starts it.
///
/// # Errors
/// A statement failed (1236 from the IO thread shows up later, in the
/// replica's status, when the replica diverged).
pub async fn repoint(ep: &NodeEndpoint, to: &ReplSource) -> Result<(), ProbeError> {
    change_master(ep, to, "slave_pos").await
}

async fn change_master(ep: &NodeEndpoint, to: &ReplSource, use_gtid: &str) -> Result<(), ProbeError> {
    let mut s = session(ep).await?;
    s.exec("STOP SLAVE").await?;
    s.exec("SET GLOBAL read_only = ON").await?;
    s.exec(&format!(
        "CHANGE MASTER TO MASTER_HOST = {}, MASTER_PORT = {}, MASTER_USER = {}, \
         MASTER_PASSWORD = {}, MASTER_USE_GTID = {use_gtid}",
        literal(&to.host),
        to.port,
        literal(&to.user),
        literal(&to.password)
    ))
    .await?;
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = OFF").await?;
    s.exec("SET GLOBAL rpl_semi_sync_slave_enabled = ON").await?;
    s.exec("START SLAVE").await?;
    s.close().await;
    Ok(())
}

/// Accounts a fence never kills: SprigDB's own and MariaDB's.
const SYSTEM_USERS: [&str; 6] = [
    "system user",
    "event_scheduler",
    "sprig_ctl",
    "sprig_repl",
    "root",
    "mariadb.sys",
];

/// The self-fence of a writer (chapter 25, "DC leases, sub-leases and
/// self-fencing" step 6): `read_only = ON`, then every session of a tenant
/// account is killed, so no open transaction commits afterwards. Returns
/// how many sessions were killed.
///
/// # Errors
/// A statement failed.
pub async fn fence_node(ep: &NodeEndpoint) -> Result<usize, ProbeError> {
    let mut s = session(ep).await?;
    s.exec("SET GLOBAL read_only = ON").await?;
    let me = s.connection_id();
    let rows = s
        .rows("SELECT ID, USER FROM information_schema.PROCESSLIST")
        .await?;
    let mut killed = 0;
    for r in rows {
        let (Some(id), Some(user)) = (col(&r, "ID"), col(&r, "USER")) else {
            continue;
        };
        if SYSTEM_USERS.contains(&user) || id == me.to_string() {
            continue;
        }
        if s.exec(&format!("KILL CONNECTION {id}")).await.is_ok() {
            killed += 1;
        }
    }
    s.close().await;
    Ok(killed)
}

/// `@@gtid_binlog_pos` of a node.
///
/// # Errors
/// The node couldn't be reached.
pub async fn binlog_pos(ep: &NodeEndpoint) -> Result<String, ProbeError> {
    let mut s = session(ep).await?;
    let p = s
        .scalar("SELECT @@gtid_binlog_pos")
        .await?
        .unwrap_or_default();
    s.close().await;
    Ok(p)
}

/// Transactions a returning node logged that the new writer lacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergedRange {
    /// The GTID domain.
    pub domain: u32,
    /// The server that logged them (the old primary).
    pub server: u32,
    /// The last sequence number the new writer had applied (exclusive).
    pub after_seq: u64,
    /// The last diverged sequence number (inclusive).
    pub through_seq: u64,
}

impl DivergedRange {
    /// How many transactions (sequence numbers rise by one per
    /// transaction under `gtid_strict_mode`).
    #[must_use]
    pub fn count(&self) -> u64 {
        self.through_seq.saturating_sub(self.after_seq)
    }

    /// Whether `g` is in the range.
    #[must_use]
    pub fn contains(&self, g: &Gtid) -> bool {
        g.domain == self.domain
            && g.server == self.server
            && g.seq > self.after_seq
            && g.seq <= self.through_seq
    }
}

/// Chapter 25 "Diverged transactions" step 1 for a node: the GTIDs in its
/// `gtid_binlog_pos` past the position the new writer had applied when it
/// was promoted (`applied`), in `domain`. `None` when the node logged
/// nothing past it.
#[must_use]
pub fn diverged(node_binlog_pos: &str, applied: &str, domain: u32) -> Option<DivergedRange> {
    let node: GtidPos = node_binlog_pos.parse().ok()?;
    let applied: GtidPos = applied.parse().unwrap_or_default();
    let g = node.get(domain)?;
    let after = applied.get(domain).map_or(0, |a| a.seq);
    (g.seq > after).then_some(DivergedRange {
        domain,
        server: g.server,
        after_seq: after,
        through_seq: g.seq,
    })
}

/// One diverged transaction, from the node's binlog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DivergedTxn {
    /// Its GTID.
    pub gtid: String,
    /// Tables it changed, `db.table`.
    pub tables: Vec<String>,
    /// The statements, where the binlog annotates row events.
    pub statements: Vec<String>,
}

/// Reads the diverged transactions from the node's binlog with `SHOW
/// BINLOG EVENTS` (step 3 of the extract; the row images are the
/// `errant_extract` job's, chapter 38's reader).
///
/// # Errors
/// The node couldn't be reached or its binlogs listed.
pub async fn diverged_events(
    ep: &NodeEndpoint,
    range: &DivergedRange,
) -> Result<Vec<DivergedTxn>, ProbeError> {
    let mut s = session(ep).await?;
    let logs = s.rows("SHOW BINARY LOGS").await?;
    let mut out: Vec<DivergedTxn> = Vec::new();
    for log in logs {
        let Some(name) = col(&log, "Log_name") else {
            continue;
        };
        let events = s
            .rows(&format!("SHOW BINLOG EVENTS IN {}", literal(name)))
            .await?;
        let mut current: Option<usize> = None;
        for e in events {
            let kind = col(&e, "Event_type").unwrap_or_default();
            let info = col(&e, "Info").unwrap_or_default();
            match kind {
                "Gtid" => {
                    current = gtid_of(info).filter(|g| range.contains(g)).map(|g| {
                        out.push(DivergedTxn {
                            gtid: g.to_string(),
                            tables: Vec::new(),
                            statements: Vec::new(),
                        });
                        out.len() - 1
                    });
                }
                "Table_map" => {
                    if let (Some(i), Some(t)) = (current, table_of(info))
                        && !out[i].tables.contains(&t)
                    {
                        out[i].tables.push(t);
                    }
                }
                "Annotate_rows" | "Query" => {
                    if let Some(i) = current
                        && !info.eq_ignore_ascii_case("COMMIT")
                        && !info.starts_with("BEGIN")
                    {
                        out[i].statements.push(info.to_owned());
                    }
                }
                "Xid" => current = None,
                _ => {}
            }
        }
    }
    s.close().await;
    Ok(out)
}

/// `BEGIN GTID 1-101-7` (or `GTID 1-101-7` for DDL) -> the GTID.
fn gtid_of(info: &str) -> Option<Gtid> {
    info.split_whitespace()
        .skip_while(|w| *w != "GTID")
        .nth(1)
        .and_then(|g| g.parse().ok())
}

/// `table_id: 33 (db.tbl)` -> `db.tbl`.
fn table_of(info: &str) -> Option<String> {
    let open = info.find('(')?;
    let close = info.rfind(')')?;
    (close > open + 1).then(|| info[open + 1..close].to_owned())
}

/// A returning former primary with nothing diverged: replicate from the
/// new primary from its own binlog position (`current_pos`: it logged its
/// writes as a primary, so its `gtid_slave_pos` is stale); `read_only`
/// stays on.
///
/// # Errors
/// A statement failed.
pub async fn rejoin(ep: &NodeEndpoint, to: &ReplSource) -> Result<(), ProbeError> {
    change_master(ep, to, "current_pos").await
}

#[cfg(test)]
mod tests;
