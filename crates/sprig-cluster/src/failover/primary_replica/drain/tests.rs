//! A deterministic simulation of a replica's threads, with MariaDB's rule
//! that starting the SQL thread while the IO thread is stopped discards the
//! relay log (under `MASTER_USE_GTID`, `Gtid_IO_Pos` drops back to
//! `gtid_slave_pos`). Time is tokio's paused clock.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Io {
    Yes,
    Connecting,
    No,
}

/// One replica of a dead (or fenced) primary in domain 1, server 101.
#[derive(Debug)]
struct Sim {
    configured: bool,
    io: Io,
    sql: bool,
    received: u64,
    applied: u64,
    /// Transactions applied per second.
    rate: u64,
    /// The SQL thread fails (stops with an error) when started.
    sql_fails: bool,
    /// Transactions the primary sends while the IO thread runs (a fenced
    /// primary's last ones in flight), delivered on the next status.
    in_flight: u64,
    /// Transactions received and then discarded.
    discarded: u64,
    log: Vec<String>,
}

impl Sim {
    fn new(io: Io, sql: bool, received: u64, applied: u64) -> Self {
        Self {
            configured: true,
            io,
            sql,
            received,
            applied,
            rate: 1000,
            sql_fails: false,
            in_flight: 0,
            discarded: 0,
            log: Vec::new(),
        }
    }
}

fn pos(seq: u64) -> String {
    if seq == 0 {
        String::new()
    } else {
        format!("1-101-{seq}")
    }
}

impl ReplicaThreads for Sim {
    async fn status(&mut self) -> Result<ReplStatus, ProbeError> {
        if self.io != Io::No && self.in_flight > 0 {
            self.received += self.in_flight;
            self.in_flight = 0;
        }
        Ok(ReplStatus {
            configured: self.configured,
            io_alive: self.io != Io::No,
            sql_running: self.sql,
            io_pos: pos(self.received),
            slave_pos: pos(self.applied),
            lag_seconds: Some(0),
            sql_error: if self.sql_fails {
                "Duplicate entry".into()
            } else {
                String::new()
            },
        })
    }

    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        self.log.push(sql.to_owned());
        match sql {
            "STOP SLAVE IO_THREAD" => self.io = Io::No,
            "STOP SLAVE" => {
                self.io = Io::No;
                self.sql = false;
            }
            "START SLAVE SQL_THREAD" => {
                if self.io == Io::No {
                    // MariaDB: the relay log is discarded.
                    self.discarded += self.received - self.applied;
                    self.received = self.applied;
                }
                self.sql = !self.sql_fails;
            }
            "RESET SLAVE ALL" => {
                self.discarded += self.received - self.applied;
                self.received = self.applied;
                self.configured = false;
            }
            other => panic!("unexpected {other}"),
        }
        Ok(())
    }

    async fn gtid_wait(&mut self, p: &GtidPos, wait: Duration) -> Result<bool, ProbeError> {
        let target = p.get(1).map_or(0, |g| g.seq);
        if target <= self.applied {
            return Ok(true);
        }
        if !self.sql {
            tokio::time::sleep(wait).await;
            return Ok(false);
        }
        let reachable = target.min(self.received);
        let need = Duration::from_millis((reachable - self.applied) * 1000 / self.rate.max(1));
        if need > wait {
            let done = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX) * self.rate / 1000;
            self.applied += done;
            tokio::time::sleep(wait).await;
            return Ok(false);
        }
        tokio::time::sleep(need).await;
        self.applied = reachable;
        Ok(self.applied >= target)
    }
}

const WAIT: Duration = Duration::from_secs(30);

#[test]
fn the_steps_follow_the_threads() {
    let st = |io_alive, sql_running, io: &str, applied: &str| ReplStatus {
        configured: true,
        io_alive,
        sql_running,
        io_pos: io.into(),
        slave_pos: applied.into(),
        ..ReplStatus::default()
    };
    assert_eq!(
        next_step(&st(false, false, "1-1-5", "1-1-5")),
        DrainStep::Done
    );
    assert_eq!(next_step(&ReplStatus::default()), DrainStep::Done);
    assert!(
        matches!(next_step(&st(true, true, "1-1-9", "1-1-5")), DrainStep::Wait(p) if p.to_string() == "1-1-9")
    );
    assert_eq!(
        next_step(&st(true, false, "1-1-9", "1-1-5")),
        DrainStep::StartSql
    );
    assert!(
        matches!(next_step(&st(false, false, "1-1-9", "1-1-5")), DrainStep::Stuck(w) if w.contains("both"))
    );
    // Received falls back to applied (never connected).
    assert_eq!(next_step(&st(false, false, "", "1-1-5")), DrainStep::Done);
}

#[tokio::test(start_paused = true)]
async fn a_stopped_sql_thread_is_started_only_while_the_io_thread_lives() {
    for io in [Io::Yes, Io::Connecting] {
        let mut r = Sim::new(io, false, 1_500, 40);
        let d = drain_on(&mut r, WAIT).await.expect("drain");
        assert!(d.stuck.is_none(), "{d:?}");
        assert_eq!((r.applied, r.discarded), (1_500, 0));
        assert_eq!(r.log, ["START SLAVE SQL_THREAD"]);
        assert_eq!(d.status.slave_pos, "1-101-1500");
    }
    // Both stopped: nothing is run, the relay log is kept, and it says why.
    let mut r = Sim::new(Io::No, false, 1_500, 40);
    let d = drain_on(&mut r, WAIT).await.expect("drain");
    assert!(
        d.stuck.as_deref().is_some_and(|w| w.contains("both")),
        "{d:?}"
    );
    assert!(r.log.is_empty());
    assert_eq!((r.received, r.discarded), (1_500, 0));
}

#[tokio::test(start_paused = true)]
async fn the_sequence_before_r2drain_discarded_the_relay_log() {
    // What the model says about the old order (STOP SLAVE IO_THREAD, then
    // the SQL thread started to apply): 1,460 received, acknowledged
    // transactions gone.
    let mut r = Sim::new(Io::Connecting, false, 1_500, 40);
    r.exec("STOP SLAVE IO_THREAD").await.expect("stop");
    r.exec("START SLAVE SQL_THREAD").await.expect("start");
    assert_eq!((r.received, r.applied, r.discarded), (40, 40, 1_460));
}

#[tokio::test(start_paused = true)]
async fn a_slow_apply_is_reported_and_nothing_is_reset() {
    // 100 per second, 10,000 behind: 100 s, over the 30 s wait.
    let mut r = Sim::new(Io::Connecting, true, 10_000, 0);
    r.rate = 100;
    let d = drain_on(&mut r, WAIT).await.expect("drain");
    assert!(
        d.stuck
            .as_deref()
            .is_some_and(|w| w.contains("within 30 s")),
        "{d:?}"
    );
    assert_eq!(r.discarded, 0);
    let out = stop_drained_on(&mut r, WAIT).await.expect("stop");
    assert!(out.is_err(), "{out:?}");
    assert!(
        !r.log
            .iter()
            .any(|l| l == "STOP SLAVE" || l == "STOP SLAVE IO_THREAD")
    );
    assert_eq!(r.discarded, 0);
    // Given the time, it finishes and stops cleanly.
    let out = stop_drained_on(&mut r, Duration::from_secs(120))
        .await
        .expect("stop");
    assert!(out.is_ok(), "{out:?}");
    assert_eq!((r.applied, r.discarded), (10_000, 0));
}

#[tokio::test(start_paused = true)]
async fn a_failing_sql_thread_is_reported() {
    let mut r = Sim::new(Io::Connecting, false, 50, 10);
    r.sql_fails = true;
    let d = drain_on(&mut r, WAIT).await.expect("drain");
    assert!(
        d.stuck
            .as_deref()
            .is_some_and(|w| w.contains("Duplicate entry")),
        "{d:?}"
    );
    assert_eq!(
        r.log,
        ["START SLAVE SQL_THREAD"],
        "started once, not retried"
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_applies_what_arrives_until_the_io_thread_stops() {
    let mut r = Sim::new(Io::Yes, true, 500, 100);
    r.in_flight = 25;
    let out = stop_drained_on(&mut r, WAIT).await.expect("stop");
    let st = out.expect("stopped");
    assert_eq!(st.slave_pos, "1-101-525");
    assert_eq!(r.log, ["STOP SLAVE IO_THREAD", "STOP SLAVE"]);
    r.exec("RESET SLAVE ALL").await.expect("reset");
    assert_eq!(r.discarded, 0);
    // A promotion retried after the reset finds nothing to stop.
    let out = stop_drained_on(&mut r, WAIT).await.expect("stop");
    assert!(out.is_ok());
    assert_eq!(r.log.len(), 3);
}

/// Every combination of thread states, backlog and speed: the drain and
/// the stop never discard a received transaction, never start the SQL
/// thread with the IO thread stopped, and either apply everything or say
/// why not with the relay log intact.
#[tokio::test(start_paused = true)]
async fn no_combination_discards_the_relay_log() {
    let mut runs = 0;
    for io in [Io::Yes, Io::Connecting, Io::No] {
        for sql in [true, false] {
            for (received, applied) in [(0, 0), (100, 100), (100, 0), (5_000, 1_000), (50_000, 10)]
            {
                for rate in [10, 1_000, 100_000] {
                    for fails in [false, true] {
                        for in_flight in [0, 7] {
                            let mut r = Sim::new(io, sql && !fails, received, applied);
                            r.rate = rate;
                            r.sql_fails = fails;
                            r.in_flight = in_flight;
                            let out = stop_drained_on(&mut r, WAIT).await.expect("stop");
                            let mut io_stopped = io == Io::No;
                            for l in &r.log {
                                match l.as_str() {
                                    "STOP SLAVE IO_THREAD" | "STOP SLAVE" => io_stopped = true,
                                    "START SLAVE SQL_THREAD" => {
                                        assert!(!io_stopped, "SQL started with IO stopped: {r:?}");
                                    }
                                    _ => {}
                                }
                            }
                            assert_eq!(r.discarded, 0, "{r:?}");
                            match out {
                                Ok(st) => {
                                    assert!(st.drained(), "{r:?}");
                                    assert_eq!(r.applied, r.received, "{r:?}");
                                }
                                Err(why) => {
                                    assert!(
                                        r.applied < r.received || r.in_flight > 0,
                                        "{why}: {r:?}"
                                    );
                                }
                            }
                            runs += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(runs, 3 * 2 * 5 * 3 * 2 * 2);
}
