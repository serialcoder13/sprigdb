//! SprigDB chooses the replica mariadb-operator promotes, and repairs the
//! replicas a failover leaves broken (chapter 25, "Primary + replicas on
//! Kubernetes"; r2opfix).
//!
//! **Why.** mariadb-operator 26.10's own failover promotes the replica with
//! the highest *applied* position, skips every replica that still has relay
//! log to apply, and breaks ties by pod name. Under `AFTER_SYNC` semi-sync an
//! acknowledged commit is only guaranteed to be in *some* replica's relay
//! log, so the replica it skips can be the only one holding it: zero loss
//! isn't guaranteed. So `replication.primary.autoFailover` is off, and the
//! SprigDB operator decides:
//!
//! ```text
//! watch(cluster), every second, for registered primary + replica clusters:
//! 1. the primary pod (status.currentPrimaryPodIndex) answers: nothing to do;
//!    repair broken replicas (below)
//! 2. it doesn't answer, and its pod is not Ready (two sources), for
//!    failover.cluster_detect_seconds:
//!    a. on every other pod: STOP SLAVE IO_THREAD; START SLAVE SQL_THREAD;
//!       MASTER_GTID_WAIT(Gtid_IO_Pos), so what it received is applied
//!    b. best = the replica whose received position contains every other's
//!       (the most advanced: it holds every acknowledged commit); none, or
//!       best not fully applied: stop and report (FailoverBlocked)
//!    c. the primary still down: patch spec.replication.primary.podIndex = best;
//!       mariadb-operator's switchover promotes it (its "new primary synced"
//!       check passes, since best's relay log is applied) and repoints the rest
//! repair(replica), with the primary answering and no switchover in flight:
//!    broken = the SQL thread stopped with 1950 (GTID out of order), or the
//!    IO thread with 1236 (mariadb-operator resets gtid_slave_pos when it
//!    repoints replicas after a failover, so a replica that logs what it
//!    applies replays the new primary's binlog from its start)
//!    safe = every domain of the replica's gtid_binlog_pos is in the primary's
//!    at the same or a higher sequence number, AND a binlog dump from the
//!    primary starting at the replica's position is accepted (the server's
//!    gtid_strict_mode check: the position is in the primary's history)
//!    safe: STOP SLAVE; SET GLOBAL gtid_slave_pos = @@gtid_binlog_pos; START SLAVE
//!    not safe: mark the node for rebuild (status.rebuild, Event NodeNeedsRebuild)
//! ```

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, Row, SslOpts};
use serde::{Deserialize, Serialize};
use sprig_binlog::{BinlogSource, BinlogStream, BinlogTls, Event, GtidPos, StreamOptions};
use sprig_common::{Result, SprigCode, SprigError};

/// How long a node may take to answer one statement here.
pub const STATEMENT_TIMEOUT: Duration = Duration::from_secs(3);
/// How long a candidate may take to apply its relay log (step 2a).
pub const DRAIN_WAIT: Duration = Duration::from_secs(20);

/// One node's replication state, as `SHOW SLAVE STATUS` and the GTID
/// variables show it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplView {
    /// The pod index.
    pub index: u8,
    /// `Master_Host`; empty when the node isn't a replica.
    pub master_host: String,
    /// `Slave_IO_Running = Yes`.
    pub io_running: bool,
    /// `Slave_SQL_Running = Yes`.
    pub sql_running: bool,
    /// `Gtid_IO_Pos`: what the IO thread received (and, under semi-sync,
    /// acknowledged).
    pub io_pos: String,
    /// `@@gtid_slave_pos`: what the SQL thread applied.
    pub slave_pos: String,
    /// `@@gtid_binlog_pos`.
    pub binlog_pos: String,
    /// `Last_SQL_Errno`.
    pub sql_errno: u32,
    /// `Last_IO_Errno`.
    pub io_errno: u32,
    /// `@@read_only`.
    pub read_only: bool,
}

impl ReplView {
    /// What the node holds of the old primary's transactions: the received
    /// position, or the applied one when nothing was received.
    #[must_use]
    pub fn received(&self) -> GtidPos {
        let io: GtidPos = self.io_pos.parse().unwrap_or_default();
        let applied: GtidPos = self.slave_pos.parse().unwrap_or_default();
        merge(&io, &applied)
    }

    /// Everything received is applied.
    #[must_use]
    pub fn drained(&self) -> bool {
        let applied: GtidPos = self.slave_pos.parse().unwrap_or_default();
        applied.reached(&self.received())
    }

    /// The failover left it broken (see the module docs).
    #[must_use]
    pub fn broken(&self) -> bool {
        !self.master_host.is_empty()
            && ((!self.sql_running && self.sql_errno == 1950)
                || (!self.io_running && self.io_errno == 1236))
    }
}

/// Per domain, the higher sequence number of the two.
#[must_use]
pub fn merge(a: &GtidPos, b: &GtidPos) -> GtidPos {
    let mut out = a.clone();
    for g in b.iter() {
        if out.get(g.domain).is_none_or(|h| h.seq < g.seq) {
            out.update(*g);
        }
    }
    out
}

/// Step 2b: the replica to promote, or why none may be.
///
/// The best replica's received position must contain every other's (it
/// holds every transaction any replica acknowledged); equal positions go to
/// the lowest index. It must also have applied all of it (step 2a), since
/// mariadb-operator's promotion discards a relay log.
///
/// # Errors
/// A message for the `FailoverBlocked` event: no replica answered, two
/// replicas diverge, or the best one couldn't apply what it received.
pub fn choose(views: &[ReplView]) -> std::result::Result<u8, String> {
    let mut sorted: Vec<&ReplView> = views.iter().collect();
    sorted.sort_by_key(|v| v.index);
    let best = sorted
        .iter()
        .copied()
        .find(|c| {
            let cp = c.received();
            sorted.iter().all(|o| cp.reached(&o.received()))
        })
        .ok_or_else(|| {
            if views.is_empty() {
                "no replica answered".to_owned()
            } else {
                format!(
                    "no replica holds every other's transactions: {}",
                    views
                        .iter()
                        .map(|v| format!("pod {} received {}", v.index, v.received()))
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            }
        })?;
    // Same sequence number from different servers: two histories.
    for o in sorted.iter().filter(|o| o.index != best.index) {
        for g in o.received().iter() {
            if let Some(b) = best.received().get(g.domain)
                && b.seq == g.seq
                && b.server != g.server
            {
                return Err(format!(
                    "pods {} ({g}) and {} ({b}) diverge",
                    o.index, best.index
                ));
            }
        }
    }
    if !best.drained() {
        return Err(format!(
            "pod {} received {} but applied only {} within {}s",
            best.index,
            best.received(),
            best.slave_pos,
            DRAIN_WAIT.as_secs()
        ));
    }
    Ok(best.index)
}

/// What to do with a broken replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// `SET GLOBAL gtid_slave_pos = @@gtid_binlog_pos` is safe.
    Apply,
    /// Not provably safe: rebuild the node.
    Rebuild(String),
}

/// The position check of the repair: every domain of `replica`'s
/// `gtid_binlog_pos` is in `primary`'s at the same or a higher sequence
/// number. `Ok` is necessary, not sufficient: the dump check
/// ([`ReplicationOps::in_history`]) proves the position is the primary's.
///
/// # Errors
/// Why the replica is ahead of, or beside, the primary.
pub fn within(replica: &GtidPos, primary: &GtidPos) -> std::result::Result<(), String> {
    for g in replica.iter() {
        match primary.get(g.domain) {
            None => {
                return Err(format!(
                    "the replica logged {g} in domain {}, which the primary has never logged",
                    g.domain
                ));
            }
            Some(p) if p.seq < g.seq => {
                return Err(format!("the replica logged {g}, past the primary's {p}"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The repair decision from the two checks.
#[must_use]
pub fn repair_plan(replica_binlog_pos: &str, primary_binlog_pos: &str, in_history: Result<bool>) -> Repair {
    let (Ok(r), Ok(p)) = (
        replica_binlog_pos.parse::<GtidPos>(),
        primary_binlog_pos.parse::<GtidPos>(),
    ) else {
        return Repair::Rebuild("unreadable GTID positions".into());
    };
    if let Err(why) = within(&r, &p) {
        return Repair::Rebuild(why);
    }
    match in_history {
        Ok(true) => Repair::Apply,
        Ok(false) => Repair::Rebuild(format!(
            "the primary's binlog doesn't contain the replica's position {r}"
        )),
        Err(e) => Repair::Rebuild(format!("the history check failed: {e}")),
    }
}

/// The SQL the decisions need, as root over TLS (a seam for tests).
#[async_trait]
pub trait ReplicationOps: Send + Sync + 'static {
    /// The node's replication state.
    ///
    /// # Errors
    /// The node didn't answer within [`STATEMENT_TIMEOUT`].
    async fn view(&self, addr: &str, root: &str, index: u8) -> Result<ReplView>;
    /// Step 2a: stop receiving, apply the relay log (waiting up to
    /// `wait`), and return the state after.
    ///
    /// # Errors
    /// The node didn't answer.
    async fn drain(&self, addr: &str, root: &str, index: u8, wait: Duration) -> Result<ReplView>;
    /// Starts the IO thread again (an abandoned failover).
    ///
    /// # Errors
    /// The node didn't answer.
    async fn resume(&self, addr: &str, root: &str) -> Result<()>;
    /// `STOP SLAVE; SET GLOBAL gtid_slave_pos = @@gtid_binlog_pos; START SLAVE`.
    ///
    /// # Errors
    /// The node refused.
    async fn repair(&self, addr: &str, root: &str) -> Result<()>;
    /// Whether `pos` is in the history of the node at `addr`: a
    /// non-blocking binlog dump starting there is accepted.
    ///
    /// # Errors
    /// The node couldn't be reached (not a "no").
    async fn in_history(&self, addr: &str, root: &str, pos: &str) -> Result<bool>;
}

/// [`ReplicationOps`] over `mysql_async` and the binlog reader, as root.
pub struct MysqlReplication {
    /// The CA of the MariaDB servers; `None`: encrypt without verifying.
    ca_file: Option<std::path::PathBuf>,
    pools: Mutex<HashMap<(String, String), Pool>>,
}

impl std::fmt::Debug for MysqlReplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlReplication")
            .field("ca_file", &self.ca_file)
            .finish_non_exhaustive()
    }
}

fn unavailable(addr: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::ClusterUnavailable, format!("{addr}: {e}"))
}

fn split(addr: &str) -> Result<(String, u16)> {
    addr.rsplit_once(':')
        .and_then(|(h, p)| Some((h.to_owned(), p.parse::<u16>().ok()?)))
        .ok_or_else(|| SprigError::internal(format!("bad node address {addr}")))
}

fn text(row: &Row, name: &str) -> String {
    row.columns_ref()
        .iter()
        .position(|c| c.name_str().eq_ignore_ascii_case(name))
        .and_then(|i| row.as_ref(i))
        .map(|v| match v {
            mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            mysql_async::Value::NULL => String::new(),
            other => other.as_sql(true).trim_matches('\'').to_owned(),
        })
        .unwrap_or_default()
}

impl MysqlReplication {
    /// Connections verified against `ca_file` (or not verified).
    #[must_use]
    pub fn new(ca_file: Option<std::path::PathBuf>) -> Self {
        Self {
            ca_file,
            pools: Mutex::new(HashMap::new()),
        }
    }

    fn pool(&self, addr: &str, root: &str) -> Result<Pool> {
        let k = (addr.to_owned(), root.to_owned());
        let mut pools = self.pools.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = pools.get(&k) {
            return Ok(p.clone());
        }
        let (host, port) = split(addr)?;
        let ssl = match &self.ca_file {
            Some(ca) => SslOpts::default().with_root_certs(vec![ca.clone().into()]),
            None => SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        };
        let constraints =
            PoolConstraints::new(0, 2).ok_or_else(|| SprigError::internal("pool constraints"))?;
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(root.to_owned()))
            .prefer_socket(false)
            .ssl_opts(Some(ssl))
            .pool_opts(
                PoolOpts::default()
                    .with_constraints(constraints)
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            );
        let p = Pool::new(opts);
        pools.insert(k, p.clone());
        Ok(p)
    }

    async fn conn(&self, addr: &str, root: &str) -> Result<mysql_async::Conn> {
        let pool = self.pool(addr, root)?;
        tokio::time::timeout(STATEMENT_TIMEOUT, pool.get_conn())
            .await
            .map_err(|_| unavailable(addr, "connect timed out"))?
            .map_err(|e| unavailable(addr, e))
    }

    async fn read(c: &mut mysql_async::Conn, addr: &str, index: u8) -> Result<ReplView> {
        let q = async {
            let status: Option<Row> = c.query_first("SHOW SLAVE STATUS").await?;
            let vars: Option<(String, String, i64)> = c
                .query_first("SELECT @@GLOBAL.gtid_slave_pos, @@GLOBAL.gtid_binlog_pos, @@GLOBAL.read_only")
                .await?;
            Ok::<_, mysql_async::Error>((status, vars))
        };
        let (status, vars) = tokio::time::timeout(STATEMENT_TIMEOUT, q)
            .await
            .map_err(|_| unavailable(addr, "timed out"))?
            .map_err(|e| unavailable(addr, e))?;
        let (slave_pos, binlog_pos, ro) = vars.unwrap_or_default();
        let mut v = ReplView {
            index,
            slave_pos,
            binlog_pos,
            read_only: ro != 0,
            ..ReplView::default()
        };
        if let Some(r) = status {
            v.master_host = text(&r, "Master_Host");
            v.io_running = text(&r, "Slave_IO_Running") == "Yes";
            v.sql_running = text(&r, "Slave_SQL_Running") == "Yes";
            v.io_pos = text(&r, "Gtid_IO_Pos");
            v.sql_errno = text(&r, "Last_SQL_Errno").parse().unwrap_or(0);
            v.io_errno = text(&r, "Last_IO_Errno").parse().unwrap_or(0);
        }
        Ok(v)
    }
}

#[async_trait]
impl ReplicationOps for MysqlReplication {
    async fn view(&self, addr: &str, root: &str, index: u8) -> Result<ReplView> {
        let mut c = self.conn(addr, root).await?;
        Self::read(&mut c, addr, index).await
    }

    async fn drain(&self, addr: &str, root: &str, index: u8, wait: Duration) -> Result<ReplView> {
        let mut c = self.conn(addr, root).await?;
        let before = Self::read(&mut c, addr, index).await?;
        if before.master_host.is_empty() {
            return Ok(before);
        }
        let step = async {
            c.query_drop("STOP SLAVE IO_THREAD").await?;
            // A stopped SQL thread (by hand, or a lag test) must apply too.
            c.query_drop("START SLAVE SQL_THREAD").await?;
            Ok::<_, mysql_async::Error>(())
        };
        tokio::time::timeout(STATEMENT_TIMEOUT, step)
            .await
            .map_err(|_| unavailable(addr, "timed out"))?
            .map_err(|e| unavailable(addr, e))?;
        let io = Self::read(&mut c, addr, index).await?;
        if !io.drained() {
            let target = io.received().to_string();
            let secs = wait.as_secs().max(1);
            let r: std::result::Result<Option<i64>, _> = tokio::time::timeout(
                wait + STATEMENT_TIMEOUT,
                c.exec_first("SELECT MASTER_GTID_WAIT(?, ?)", (target, secs)),
            )
            .await
            .map_err(|_| unavailable(addr, "MASTER_GTID_WAIT timed out"))?;
            r.map_err(|e| unavailable(addr, e))?;
        }
        Self::read(&mut c, addr, index).await
    }

    async fn resume(&self, addr: &str, root: &str) -> Result<()> {
        let mut c = self.conn(addr, root).await?;
        c.query_drop("START SLAVE")
            .await
            .map_err(|e| unavailable(addr, e))
    }

    async fn repair(&self, addr: &str, root: &str) -> Result<()> {
        let mut c = self.conn(addr, root).await?;
        for q in [
            "STOP SLAVE",
            "SET GLOBAL gtid_slave_pos = @@GLOBAL.gtid_binlog_pos",
            "START SLAVE",
        ] {
            tokio::time::timeout(STATEMENT_TIMEOUT, c.query_drop(q))
                .await
                .map_err(|_| unavailable(addr, "timed out"))?
                .map_err(|e| unavailable(addr, e))?;
        }
        Ok(())
    }

    async fn in_history(&self, addr: &str, root: &str, pos: &str) -> Result<bool> {
        let from: GtidPos = pos.parse()?;
        let (host, port) = split(addr)?;
        let tls = match &self.ca_file {
            Some(ca) => BinlogTls::verify(Some(ca.clone())),
            None => BinlogTls::skip_verify(),
        };
        let src = BinlogSource::new(host, port, "root", root).with_tls(tls);
        // A dump id in the dump probe's range (chapter 09), so it can't
        // end a mover's or the feed's stream.
        let mut o = StreamOptions::new(sprig_cluster::probe::dump::PROBE_SERVER_ID_BASE + 7);
        o.non_blocking = true;
        o.connect_timeout = STATEMENT_TIMEOUT;
        let mut s = match BinlogStream::connect(&src, &from, &o).await {
            Ok(s) => s,
            // 1236: the position isn't in this server's binlog (strict mode).
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1236, .. }) => {
                tracing::info!(addr, pos, error = %e, "position not in the primary's history");
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        // The server checks the position before sending anything; reading to
        // the format description confirms the dump started.
        for _ in 0..8 {
            match tokio::time::timeout(STATEMENT_TIMEOUT, s.next()).await {
                Ok(Ok(Event::FormatDescription { .. } | Event::DumpEnd)) => return Ok(true),
                Ok(Ok(_)) => {}
                Ok(Err(e)) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1236, .. }) => {
                    tracing::info!(addr, pos, error = %e, "position not in the primary's history");
                    return Ok(false);
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(unavailable(addr, "binlog dump timed out")),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(index: u8, io: &str, applied: &str) -> ReplView {
        ReplView {
            index,
            master_host: "p".into(),
            io_running: false,
            sql_running: true,
            io_pos: io.into(),
            slave_pos: applied.into(),
            binlog_pos: applied.into(),
            ..ReplView::default()
        }
    }

    #[test]
    fn the_most_advanced_replica_wins_not_the_next_index() {
        // Pod 1 is the next index but received less; pod 2 holds the last
        // acknowledged commits (both drained).
        let views = [v(1, "5-10-100", "5-10-100"), v(2, "5-10-130", "5-10-130")];
        assert_eq!(choose(&views), Ok(2));
        // Equal positions: the lowest index.
        let views = [v(2, "5-10-130", "5-10-130"), v(1, "5-10-130", "5-10-130")];
        assert_eq!(choose(&views), Ok(1));
        // Several domains (early writes in domain 0 before the number was
        // set): containment in every domain.
        let views = [
            v(1, "0-10-4,5-10-100", "0-10-4,5-10-100"),
            v(2, "0-10-4,5-10-130", "0-10-4,5-10-130"),
        ];
        assert_eq!(choose(&views), Ok(2));
    }

    #[test]
    fn an_undrained_best_or_a_divergence_blocks() {
        // Pod 2 received more but couldn't apply it: promoting pod 1 would
        // lose pod 2's acknowledged commits, promoting pod 2 would discard
        // its relay log. Stop.
        let views = [v(1, "5-10-100", "5-10-100"), v(2, "5-10-130", "5-10-120")];
        let e = choose(&views).unwrap_err();
        assert!(e.contains("applied only 5-10-120"), "{e}");
        // Same sequence number, different servers.
        let views = [v(1, "5-10-100", "5-10-100"), v(2, "5-11-100", "5-11-100")];
        assert!(choose(&views).unwrap_err().contains("diverge"));
        // Neither contains the other.
        let views = [
            v(1, "0-10-9,5-10-100", "0-10-9,5-10-100"),
            v(2, "0-10-4,5-10-130", "0-10-4,5-10-130"),
        ];
        assert!(choose(&views).unwrap_err().contains("no replica holds"));
        assert_eq!(choose(&[]), Err("no replica answered".into()));
    }

    #[test]
    fn received_falls_back_to_applied() {
        // A replica that was never connected has no Gtid_IO_Pos.
        let r = v(1, "", "5-10-7");
        assert_eq!(r.received().to_string(), "5-10-7");
        assert!(r.drained());
        assert!(!v(1, "5-10-9", "5-10-7").drained());
    }

    #[test]
    fn broken_replicas_are_recognised() {
        let mut r = v(2, "", "");
        assert!(!r.broken());
        r.sql_errno = 1950;
        r.sql_running = false;
        assert!(r.broken());
        let mut r = v(2, "", "");
        r.io_errno = 1236;
        assert!(!r.broken(), "a running IO thread retries");
        r.io_running = false;
        assert!(r.broken());
        // Not a replica at all: nothing to repair.
        let mut p = ReplView::default();
        p.sql_errno = 1950;
        assert!(!p.broken());
    }

    #[test]
    fn repair_only_when_the_replica_is_a_prefix_of_the_primary() {
        // Behind the primary in every domain, and the dump accepted it.
        assert_eq!(
            repair_plan("0-10-4,7-11-250", "0-10-4,7-12-300", Ok(true)),
            Repair::Apply
        );
        // Equal.
        assert_eq!(repair_plan("7-11-250", "7-11-250", Ok(true)), Repair::Apply);
        // Ahead of the primary: the old primary's unacknowledged tail.
        assert!(matches!(
            repair_plan("7-11-260", "7-12-250", Ok(true)),
            Repair::Rebuild(w) if w.contains("past the primary")
        ));
        // A domain the primary never logged (a boot write in domain 0).
        assert!(matches!(
            repair_plan("0-11-48,7-11-250", "7-12-300", Ok(true)),
            Repair::Rebuild(w) if w.contains("domain 0")
        ));
        // Within by numbers, but the dump refused the position (a
        // different transaction at that number).
        assert!(matches!(
            repair_plan("7-11-250", "7-12-300", Ok(false)),
            Repair::Rebuild(w) if w.contains("doesn't contain")
        ));
        assert!(matches!(
            repair_plan("7-11-250", "7-12-300", Err(SprigError::internal("x"))),
            Repair::Rebuild(_)
        ));
        assert!(matches!(repair_plan("junk", "7-1-1", Ok(true)), Repair::Rebuild(_)));
    }
}
