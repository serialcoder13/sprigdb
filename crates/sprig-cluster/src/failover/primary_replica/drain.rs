//! Relay-log safe drains.
//!
//! MariaDB discards a replica's relay log when its SQL thread is started
//! while its IO thread is stopped (under `MASTER_USE_GTID` it then restarts
//! from `gtid_slave_pos` and resets `Gtid_IO_Pos`). With the primary gone,
//! what was received but not applied is lost, and under `AFTER_SYNC` that
//! can be the only copy of acknowledged commits. So:
//!
//! ```text
//! drain(replica, wait):
//!   loop until wait runs out:
//!     everything received is applied (gtid_slave_pos reached Gtid_IO_Pos) -> done
//!     SQL thread running -> MASTER_GTID_WAIT(received); look again (more may
//!       have arrived: the IO thread keeps running)
//!     SQL stopped, IO thread alive (Yes or Connecting) -> START SLAVE SQL_THREAD
//!     both stopped -> stuck: never start the SQL thread; the replica isn't
//!       drained and blocks the promotion if it holds the most
//! stop(replica, wait), before RESET SLAVE ALL (promotion, a standby's stop):
//!   drain; not drained -> refuse
//!   SQL thread running: STOP SLAVE IO_THREAD (the SQL thread goes on), then
//!     MASTER_GTID_WAIT(the final Gtid_IO_Pos)
//!   STOP SLAVE; everything received applied, or refuse (nothing is reset)
//! ```
//!
//! The IO thread is stopped only while the SQL thread runs, and the SQL
//! thread is never started while the IO thread is stopped.

use std::time::Duration;

use tokio::time::Instant;

use sprig_binlog::gtid::GtidPos;

use crate::sql::{ProbeError, ProbeSession, TextRow, literal};

/// One replica's threads and positions, from `SHOW SLAVE STATUS` and
/// `@@gtid_slave_pos`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplStatus {
    /// `SHOW SLAVE STATUS` has a row: the node is (or was) a replica.
    pub configured: bool,
    /// `Slave_IO_Running` is `Yes` or `Connecting`.
    pub io_alive: bool,
    /// `Slave_SQL_Running = Yes`.
    pub sql_running: bool,
    /// `Gtid_IO_Pos`: what the IO thread received (and, under semi-sync,
    /// acknowledged).
    pub io_pos: String,
    /// `@@gtid_slave_pos`: what the SQL thread applied.
    pub slave_pos: String,
    /// `Seconds_Behind_Master`.
    pub lag_seconds: Option<u64>,
    /// `Last_SQL_Error`, when the SQL thread stopped on one.
    pub sql_error: String,
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

impl ReplStatus {
    /// What the SQL thread applied.
    #[must_use]
    pub fn applied(&self) -> GtidPos {
        self.slave_pos.parse().unwrap_or_default()
    }

    /// What the node holds of its primary's transactions: received or
    /// applied (a replica that never connected has no `Gtid_IO_Pos`).
    #[must_use]
    pub fn received(&self) -> GtidPos {
        merge(&self.io_pos.parse().unwrap_or_default(), &self.applied())
    }

    /// Everything received is applied.
    #[must_use]
    pub fn drained(&self) -> bool {
        self.applied().reached(&self.received())
    }
}

/// What a drain does next with a replica in `st`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainStep {
    /// Everything received is applied (or the node isn't a replica).
    Done,
    /// The SQL thread runs: wait for it to apply this position.
    Wait(GtidPos),
    /// The SQL thread is stopped and the IO thread alive: start it.
    StartSql,
    /// Both threads are stopped with relay log left: starting the SQL
    /// thread would discard it, so nothing is done.
    Stuck(String),
}

/// The next step of a drain (see the module docs).
#[must_use]
pub fn next_step(st: &ReplStatus) -> DrainStep {
    if !st.configured || st.drained() {
        return DrainStep::Done;
    }
    if st.sql_running {
        return DrainStep::Wait(st.received());
    }
    if st.io_alive {
        return DrainStep::StartSql;
    }
    DrainStep::Stuck(format!(
        "both replication threads are stopped with relay log unapplied (received {}, applied \
         {}); starting the SQL thread now would make MariaDB discard it",
        st.received(),
        st.applied()
    ))
}

/// The statements a drain needs (a seam for the deterministic tests).
pub(crate) trait ReplicaThreads {
    /// `SHOW SLAVE STATUS` and `@@gtid_slave_pos`.
    async fn status(&mut self) -> Result<ReplStatus, ProbeError>;
    /// `STOP SLAVE IO_THREAD`, `START SLAVE SQL_THREAD`, `STOP SLAVE`.
    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError>;
    /// `MASTER_GTID_WAIT(pos, secs)`: whether `pos` was applied in time.
    async fn gtid_wait(&mut self, pos: &GtidPos, wait: Duration) -> Result<bool, ProbeError>;
}

/// How a drain ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    /// The replica's state at the end.
    pub status: ReplStatus,
    /// Why it isn't drained; `None` when everything received is applied.
    pub stuck: Option<String>,
}

/// Applies what the replica received without ever starting its SQL thread
/// while the IO thread is stopped (see the module docs). Stops nothing.
pub(crate) async fn drain_on<T: ReplicaThreads>(
    t: &mut T,
    wait: Duration,
) -> Result<Drained, ProbeError> {
    let deadline = Instant::now() + wait;
    let mut started = false;
    loop {
        let st = t.status().await?;
        let left = deadline.saturating_duration_since(Instant::now());
        match next_step(&st) {
            DrainStep::Done => {
                return Ok(Drained {
                    status: st,
                    stuck: None,
                });
            }
            DrainStep::Stuck(why) => {
                return Ok(Drained {
                    status: st,
                    stuck: Some(why),
                });
            }
            DrainStep::StartSql if !started => {
                t.exec("START SLAVE SQL_THREAD").await?;
                started = true;
            }
            DrainStep::StartSql => {
                return Ok(Drained {
                    stuck: Some(format!(
                        "the SQL thread doesn't stay running: {}",
                        if st.sql_error.is_empty() {
                            "no error reported"
                        } else {
                            &st.sql_error
                        }
                    )),
                    status: st,
                });
            }
            DrainStep::Wait(_) if left.is_zero() => {
                return Ok(Drained {
                    stuck: Some(format!(
                        "received {} but applied only {} within {} s",
                        st.received(),
                        st.applied(),
                        wait.as_secs()
                    )),
                    status: st,
                });
            }
            DrainStep::Wait(pos) => {
                // Timed out or not, look again: more may have arrived, or
                // the SQL thread may have stopped on an error.
                t.gtid_wait(&pos, left).await?;
            }
        }
    }
}

/// Drains, then stops replication with nothing received left unapplied,
/// so `RESET SLAVE ALL` discards nothing. `Ok(Err(why))` when it can't:
/// nothing was reset, and the caller blocks or retries.
pub(crate) async fn stop_drained_on<T: ReplicaThreads>(
    t: &mut T,
    wait: Duration,
) -> Result<Result<ReplStatus, String>, ProbeError> {
    let d = drain_on(t, wait).await?;
    if !d.status.configured {
        return Ok(Ok(d.status));
    }
    if let Some(why) = d.stuck {
        return Ok(Err(why));
    }
    if d.status.sql_running {
        // The SQL thread goes on applying; the IO thread receives nothing
        // more from here, so the target below is final.
        t.exec("STOP SLAVE IO_THREAD").await?;
        // With the IO thread stopped the drain only waits (it never
        // starts the SQL thread now); a retry finds the same state.
        if let Some(why) = drain_on(t, wait).await?.stuck {
            return Ok(Err(format!("after the IO thread stopped: {why}")));
        }
    }
    t.exec("STOP SLAVE").await?;
    let st = t.status().await?;
    if !st.drained() {
        return Ok(Err(format!(
            "received {} but applied only {} when replication stopped; nothing was reset",
            st.received(),
            st.applied()
        )));
    }
    Ok(Ok(st))
}

fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

impl ReplicaThreads for ProbeSession {
    async fn status(&mut self) -> Result<ReplStatus, ProbeError> {
        let rows = self.rows("SHOW SLAVE STATUS").await?;
        let slave_pos = self
            .scalar("SELECT @@GLOBAL.gtid_slave_pos")
            .await?
            .unwrap_or_default();
        let Some(r) = rows.first() else {
            return Ok(ReplStatus {
                slave_pos,
                ..ReplStatus::default()
            });
        };
        let io = col(r, "Slave_IO_Running").unwrap_or_default();
        Ok(ReplStatus {
            configured: true,
            io_alive: io.eq_ignore_ascii_case("Yes") || io.eq_ignore_ascii_case("Connecting"),
            sql_running: col(r, "Slave_SQL_Running").is_some_and(|v| v.eq_ignore_ascii_case("Yes")),
            io_pos: col(r, "Gtid_IO_Pos").unwrap_or_default().to_owned(),
            slave_pos,
            lag_seconds: col(r, "Seconds_Behind_Master").and_then(|v| v.parse().ok()),
            sql_error: col(r, "Last_SQL_Error").unwrap_or_default().to_owned(),
        })
    }

    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        ProbeSession::exec(self, sql).await
    }

    async fn gtid_wait(&mut self, pos: &GtidPos, wait: Duration) -> Result<bool, ProbeError> {
        // Within the session's own statement timeout; the drain loops.
        let cap = self.timeout().saturating_sub(Duration::from_secs(1));
        let secs = wait.min(cap).as_secs().max(1);
        let r = self
            .scalar(&format!(
                "SELECT MASTER_GTID_WAIT({}, {secs})",
                literal(&pos.to_string())
            ))
            .await?;
        Ok(r.as_deref() == Some("0"))
    }
}

/// [`drain_on`] over a session (the standby stop of a DC failover).
///
/// # Errors
/// A statement failed.
pub async fn drain(s: &mut ProbeSession, wait: Duration) -> Result<Drained, ProbeError> {
    drain_on(s, wait).await
}

/// [`stop_drained_on`] over a session: the replication stopped with
/// everything received applied (`Ok(Ok(status))`), or why it wasn't
/// stopped safely (`Ok(Err(why))`; nothing was reset).
///
/// # Errors
/// A statement failed.
pub async fn stop_drained(
    s: &mut ProbeSession,
    wait: Duration,
) -> Result<Result<ReplStatus, String>, ProbeError> {
    stop_drained_on(s, wait).await
}

#[cfg(test)]
mod tests;
