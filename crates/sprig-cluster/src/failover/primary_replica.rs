//! Primary + replicas, driven by SprigDB (failover steps 3-10).
//!
//! ```text
//! 3. on each replica whose agent answers: apply what it received, with the
//!    IO thread left running (the primary is fenced or down, so nothing new
//!    arrives): START SLAVE SQL_THREAD only while the IO thread is alive,
//!    MASTER_GTID_WAIT(received, 30) ([`drain`])
//! 4. best = the replica whose received position contains every other's; none,
//!    two that diverge, or best not fully applied (both threads stopped with
//!    relay log left, or too slow) -> stop and alert
//! 5. expected loss: at most 1 s if Rpl_semi_sync_master_status was ON in the last
//!    per-second sample (AFTER_SYNC); otherwise best's last measured lag
//! 6. loss > rpo, or failover.mode manual -> stop, alert, wait for an operator
//! 8. best: drain again; STOP SLAVE IO_THREAD; MASTER_GTID_WAIT; STOP SLAVE;
//!    everything received applied, or refuse; RESET SLAVE ALL;
//!    rpl_semi_sync_master_enabled = ON; read_only = OFF
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

pub mod drain;

pub use drain::{DrainStep, Drained, ReplStatus, next_step};

/// How long a replica may take to apply its relay log (steps 3 and 8).
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
    /// What it received: `Gtid_IO_Pos`, or the applied position where that
    /// is higher. The choice compares these.
    #[serde(default)]
    pub received: String,
    /// `Seconds_Behind_Master` before the drain.
    pub lag_seconds: Option<u64>,
    /// Whether everything received was applied within the wait.
    pub drained: bool,
    /// Why it wasn't drained (both threads stopped, too slow, the SQL
    /// thread failing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stuck: Option<String>,
}

impl ReplicaState {
    fn received(&self) -> GtidPos {
        let applied: GtidPos = self.slave_pos.parse().unwrap_or_default();
        drain::merge(&self.received.parse().unwrap_or_default(), &applied)
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

/// Step 3 on one replica: apply what it received without discarding its
/// relay log ([`drain`]: the IO thread keeps running, and the SQL thread is
/// started only while the IO thread is alive), and read the positions.
/// Stops nothing.
///
/// # Errors
/// The replica couldn't be reached or a statement failed.
pub async fn drain_relay(ep: &NodeEndpoint) -> Result<ReplicaState, ProbeError> {
    let mut s = session(ep).await?;
    let d = drain::drain(&mut s, Duration::from_secs(RELAY_WAIT_SECONDS.into())).await?;
    s.close().await;
    let lag_seconds = d.status.lag_seconds;
    Ok(ReplicaState {
        node: ep.node.clone(),
        received: d.status.received().to_string(),
        slave_pos: d.status.slave_pos,
        lag_seconds,
        drained: d.stuck.is_none(),
        stuck: d.stuck,
    })
}

/// Step 4: the replica whose received position contains every other's (it
/// holds every transaction any replica acknowledged), preferring one that
/// applied it all, then the lowest node id. Same sequence numbers from
/// different servers mean two replicas diverge. The best one must have
/// applied everything it received: promoting it otherwise would discard
/// its relay log, and promoting another would lose what only it holds.
#[must_use]
pub fn choose(states: &[ReplicaState], domain: u32) -> Choice {
    if states.is_empty() {
        return Choice::Blocked("no replica answered".into());
    }
    let mut sorted: Vec<&ReplicaState> = states.iter().collect();
    sorted.sort_by(|a, b| b.drained.cmp(&a.drained).then_with(|| a.node.cmp(&b.node)));
    let Some(best) = sorted.iter().copied().find(|c| {
        let cp = c.received();
        sorted.iter().all(|o| cp.reached(&o.received()))
    }) else {
        return Choice::Blocked(format!(
            "no replica holds every other's transactions: {}",
            states
                .iter()
                .map(|st| format!("{} received {}", st.node, st.received()))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    };
    let bp = best.received();
    for other in states.iter().filter(|s| s.node != best.node) {
        for o in other.received().iter() {
            if let Some(b) = bp.get(o.domain)
                && b.seq == o.seq
                && b.server != o.server
            {
                return Choice::Blocked(format!(
                    "replicas {} ({o}) and {} ({b}) diverge",
                    other.node, best.node
                ));
            }
        }
    }
    if !best.drained {
        return Choice::Blocked(format!(
            "{} holds the most (received {bp}) but applied only {}: {}",
            best.node,
            best.slave_pos,
            best.stuck.as_deref().unwrap_or("not drained")
        ));
    }
    Choice::Promote(Candidate {
        node: best.node.clone(),
        applied: best.slave_pos.clone(),
        expected_loss: None,
        why: format!(
            "received position contains every other replica's; in domain {domain}: {}",
            bp.get(domain).map(ToString::to_string).unwrap_or_default()
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

/// Step 8's stop: replication stopped with everything received applied
/// ([`drain::stop_drained`]), so `RESET SLAVE ALL` discards nothing; an
/// error, with nothing reset, otherwise.
async fn stop_replication(s: &mut ProbeSession) -> Result<(), ProbeError> {
    match drain::stop_drained(s, Duration::from_secs(RELAY_WAIT_SECONDS.into())).await? {
        Ok(_) => Ok(()),
        Err(why) => Err(ProbeError::other(format!(
            "not promoted, the relay log would be lost: {why}"
        ))),
    }
}

/// Step 8: makes `ep` the primary. Call only while its agent holds the
/// writer lease and a DC permit. Returns its applied position.
///
/// # Errors
/// A statement failed; the node may be half promoted (the steps are
/// idempotent, so the caller retries).
pub async fn promote(ep: &NodeEndpoint) -> Result<String, ProbeError> {
    let mut s = session(ep).await?;
    stop_replication(&mut s).await?;
    s.exec("RESET SLAVE ALL").await?;
    s.exec("SET GLOBAL rpl_semi_sync_slave_enabled = OFF")
        .await?;
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = ON")
        .await?;
    s.exec("SET GLOBAL read_only = OFF").await?;
    let pos = s
        .scalar("SELECT @@gtid_slave_pos")
        .await?
        .unwrap_or_default();
    s.close().await;
    Ok(pos)
}

/// Step 8 where agents fence the nodes: makes
/// `ep` a primary but leaves `read_only` to its agent, which clears it
/// once it holds the writer lease and a DC permit and sees the node no
/// longer replicates. `semisync`: switch the primary side of semi-sync on
/// (only when replicas will follow it; without any, the first commit would
/// wait out the whole timeout). Returns its applied position.
///
/// # Errors
/// A statement failed; the steps are idempotent, so the caller retries.
pub async fn promote_fenced(ep: &NodeEndpoint, semisync: bool) -> Result<String, ProbeError> {
    let mut s = session(ep).await?;
    stop_replication(&mut s).await?;
    let pos = s
        .scalar("SELECT @@gtid_slave_pos")
        .await?
        .unwrap_or_default();
    s.exec("RESET SLAVE ALL").await?;
    let _ = s.exec("SET GLOBAL rpl_semi_sync_slave_enabled = OFF").await;
    let _ = s
        .exec(&format!(
            "SET GLOBAL rpl_semi_sync_master_enabled = {}",
            if semisync { "ON" } else { "OFF" }
        ))
        .await;
    s.close().await;
    Ok(pos)
}

/// Where replicas reach `ep`: its `report_host` and `report_port` when
/// set (MariaDB's own "address for replicas"), else `None` and the caller
/// uses the registered address.
///
/// # Errors
/// The node couldn't be reached.
pub async fn report_address(ep: &NodeEndpoint) -> Result<Option<(String, u16)>, ProbeError> {
    let mut s = session(ep).await?;
    let rows = s
        .rows("SELECT @@report_host AS h, @@report_port AS p")
        .await?;
    s.close().await;
    let Some(r) = rows.first() else {
        return Ok(None);
    };
    let host = col(r, "h").unwrap_or_default().trim().to_owned();
    if host.is_empty() {
        return Ok(None);
    }
    let port = col(r, "p").and_then(|p| p.parse().ok()).unwrap_or(3306);
    Ok(Some((host, port)))
}

/// Replicas reading this node's binlog now (its `Binlog Dump` threads).
///
/// # Errors
/// A statement failed.
pub async fn attached_replicas(s: &mut ProbeSession) -> Result<u32, ProbeError> {
    Ok(s.scalar(
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST \
             WHERE COMMAND LIKE 'Binlog Dump%'",
    )
    .await?
    .and_then(|v| v.parse().ok())
    .unwrap_or(0))
}

/// Whether the primary side of semi-sync is switched on (`false` where
/// the variable doesn't exist).
///
/// # Errors
/// The node couldn't be reached.
pub async fn semisync_master_enabled(s: &mut ProbeSession) -> Result<bool, ProbeError> {
    match s
        .scalar("SELECT @@global.rpl_semi_sync_master_enabled")
        .await
    {
        Ok(v) => Ok(v.is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("on"))),
        Err(e) if e.is_server_error() => Ok(false),
        Err(e) => Err(e),
    }
}

/// The semi-sync guard of a node about to take writes: a primary with semi-sync on and no replica
/// attached would make its first commit wait out
/// `rpl_semi_sync_master_timeout` (a returning primary whose standby was
/// promoted, a promoted replica whose peers haven't reattached). It is
/// switched off; returns whether it was, so the caller switches it back on
/// with [`semisync_resume`] once a replica attaches.
///
/// # Errors
/// A statement failed.
pub async fn semisync_guard(s: &mut ProbeSession) -> Result<bool, ProbeError> {
    if !semisync_master_enabled(s).await? || attached_replicas(s).await? > 0 {
        return Ok(false);
    }
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = OFF")
        .await?;
    Ok(true)
}

/// Switches the primary side of semi-sync back on once a replica is
/// attached; returns whether it did.
///
/// # Errors
/// A statement failed.
pub async fn semisync_resume(s: &mut ProbeSession) -> Result<bool, ProbeError> {
    if attached_replicas(s).await? == 0 {
        return Ok(false);
    }
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = ON")
        .await?;
    Ok(true)
}

/// Step 9: points a replica at the new primary and starts it.
///
/// # Errors
/// A statement failed (1236 from the IO thread shows up later, in the
/// replica's status, when the replica diverged).
pub async fn repoint(ep: &NodeEndpoint, to: &ReplSource) -> Result<(), ProbeError> {
    change_master(ep, to, "slave_pos").await
}

async fn change_master(
    ep: &NodeEndpoint,
    to: &ReplSource,
    use_gtid: &str,
) -> Result<(), ProbeError> {
    let mut s = session(ep).await?;
    s.exec("STOP SLAVE").await?;
    s.exec("SET GLOBAL read_only = ON").await?;
    let ssl = ssl_clause(s.scalar("SELECT @@GLOBAL.ssl_ca").await?.as_deref());
    s.exec(&format!(
        "CHANGE MASTER TO MASTER_HOST = {}, MASTER_PORT = {}, MASTER_USER = {}, \
         MASTER_PASSWORD = {}, MASTER_USE_GTID = {use_gtid}{ssl}",
        literal(&to.host),
        to.port,
        literal(&to.user),
        literal(&to.password),
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

/// The TLS part of `CHANGE MASTER`: a node that serves
/// TLS with a CA file (`ssl_ca`) checks the new primary's certificate
/// against that CA, so a former primary rejoining a cluster whose nodes
/// require TLS (`require_secure_transport`, `REQUIRE SSL` accounts)
/// replicates over verified TLS. A node without `ssl_ca` keeps what it had
/// (MariaDB 11.8 replicates over TLS by default, verifying against the
/// system's roots).
fn ssl_clause(ssl_ca: Option<&str>) -> String {
    match ssl_ca.map(str::trim) {
        Some(ca) if !ca.is_empty() => format!(", MASTER_SSL = 1, MASTER_SSL_CA = {}", literal(ca)),
        _ => String::new(),
    }
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

/// The self-fence of a writer: `read_only = ON`, then every session of a tenant
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

/// Step 1 of the diverged-transaction report for a node: the GTIDs in its
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
/// `errant_extract` job's, read with the change feed's reader).
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
