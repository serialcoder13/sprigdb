//! The binlog dump probe, run when an external cluster is registered.
//!
//! The variable checks in [`super::feed`] say what the server is configured
//! to do; this probe says whether the change feed's reader can actually
//! read the binlog. As `sprig_repl` it opens a non-blocking dump
//! ([`sprig_binlog::StreamOptions::non_blocking`]) at the node's current
//! position (`@@gtid_binlog_pos`, as the probe read it a moment earlier),
//! reads the format description and the events after it (the start
//! position's `GTID_LIST`, then whatever was committed since), verifying
//! checksums and decoding each event, until the server ends the dump, and
//! disconnects. At most [`MAX_EVENTS`] events are read, so on a busy node
//! the probe still costs one short connection.
//!
//! What it catches that the variables can't: an account without
//! `REPLICATION SLAVE` (1227), a binlog that isn't open, a start position
//! the server can't find, a checksum algorithm or event format the reader
//! doesn't know, corrupt events, an authentication plugin the reader
//! doesn't speak, and a TLS failure (a server without TLS, a certificate
//! the credential's CA doesn't sign, or a plain-text source refused by
//! `require_secure_transport`). The reader uses the endpoint's TLS: the
//! credential's `tls` and `ca_file`. Compressed binlogs (`log_bin_compress`) decode
//! and pass, and so do encrypted ones (`encrypt_binlog = ON`): the server
//! decrypts the events before sending them, and the stream skips the
//! file's `START_ENCRYPTION` event it sends first (measured on 12.3).
//!
//! **Server id.** A dump presents a replica `server_id`, and MariaDB ends
//! an older dump with the same id. Mover and feed streams take ids from
//! `mover.server_id_base` (4000000000) upwards, held in etcd;
//! the probe uses a random id in `[4200000000, 4290000000)`, which they
//! never reach, so it can't end a real stream.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_binlog::consts::event_type;
use sprig_binlog::{BinlogSource, BinlogStream, Event, GtidPos, StreamOptions};
use sprig_common::ids::JobId;
use sprig_common::{ErrorKind, SprigCode, SprigError};

use super::feed::{FeedGap, GapSeverity};
use crate::sql::NodeEndpoint;

/// The first server id the probe may present.
pub const PROBE_SERVER_ID_BASE: u32 = 4_200_000_000;
/// How many ids follow [`PROBE_SERVER_ID_BASE`].
pub const PROBE_SERVER_ID_SPAN: u32 = 90_000_000;
/// The most events the probe reads after the format description before it
/// stops (and passes) without waiting for the end of the dump.
pub const MAX_EVENTS: usize = 256;

/// What went wrong in a dump probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DumpFailureKind {
    /// The node couldn't be reached or the connection broke.
    Unreachable,
    /// The probe didn't finish within the probe timeout.
    Timeout,
    /// `sprig_repl` couldn't log in (1045).
    Login,
    /// `sprig_repl` lacks `REPLICATION SLAVE` (1227).
    Privilege,
    /// The binary log isn't open.
    NoBinlog,
    /// The server couldn't start the dump at the position (1236).
    Position,
    /// TLS failed, or the server refuses a plain-text connection.
    Transport,
    /// The binlog uses something the reader doesn't support: a checksum
    /// algorithm, an event format or an authentication plugin.
    Unsupported,
    /// An event failed its checksum or couldn't be decoded.
    Corrupt,
    /// The dump ended before the format description.
    Incomplete,
    /// Any other server error.
    Other,
}

impl DumpFailureKind {
    /// Whether the failure says something about the node's binlog or the
    /// account. A timeout or a lost connection doesn't: the profile then
    /// treats the probe as not run, rather than turning the feed off until
    /// the next deep probe.
    #[must_use]
    pub fn conclusive(self) -> bool {
        !matches!(
            self,
            DumpFailureKind::Unreachable | DumpFailureKind::Timeout
        )
    }
}

/// Why a dump probe failed, with the fix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DumpFailure {
    /// The kind of failure.
    pub kind: DumpFailureKind,
    /// MariaDB's error number, for server errors.
    pub errno: Option<u16>,
    /// What happened (internal detail; never sent to clients).
    pub detail: String,
    /// The statement or configuration change that fixes it.
    pub fix: String,
}

/// The result of one dump probe.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DumpProbe {
    /// The GTID position the dump started from.
    pub from: String,
    /// The events read, by type name ([`event_type::name`]), in order;
    /// `dump_end` when the server ended the non-blocking dump.
    pub events: Vec<String>,
    /// The server version in the format description.
    pub server_version: Option<String>,
    /// Whether events carry CRC32 checksums.
    pub checksums: Option<bool>,
    /// How long the probe took.
    pub took_ms: u64,
    /// Why it failed; `None` when it passed.
    pub failure: Option<DumpFailure>,
}

impl DumpProbe {
    /// The dump read the format description and the events after it.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }

    /// The dump failed in a way that disables the feed and the mover: a
    /// conclusive failure ([`DumpFailureKind::conclusive`]).
    #[must_use]
    pub fn blocks_feed(&self) -> bool {
        self.failure.as_ref().is_some_and(|f| f.kind.conclusive())
    }

    /// The failure as a blocking feed gap, for `sprigctl cluster show` and
    /// the registration report. `None` when the probe passed.
    #[must_use]
    pub fn gap(&self) -> Option<FeedGap> {
        let f = self.failure.as_ref()?;
        Some(FeedGap {
            variable: "binlog dump".to_owned(),
            needed: "sprig_repl reads the binlog".to_owned(),
            found: f.detail.clone(),
            severity: GapSeverity::Blocking,
            fix: f.fix.clone(),
        })
    }

    /// One line for the registration report: what failed and the fix.
    #[must_use]
    pub fn advisory(&self) -> Option<String> {
        let f = self.failure.as_ref()?;
        Some(format!(
            "the change feed can't read the binlog as sprig_repl ({}): {}",
            f.detail, f.fix
        ))
    }
}

/// Opens a non-blocking dump on `ep` (the `sprig_repl` endpoint) after
/// `from`, reads the format description and the events after it until the
/// dump ends (at most [`MAX_EVENTS`]), and disconnects. The whole probe is bounded by `timeout`
/// (`cluster.probe_timeout_ms`). Never fails: failures are in the result.
///
/// The reader uses `ep`'s TLS options ([`NodeEndpoint::binlog_source`]):
/// the credential's CA and verification mode.
pub async fn run(ep: &NodeEndpoint, from: &str, timeout: Duration) -> DumpProbe {
    let start = Instant::now();
    let mut out = DumpProbe {
        from: from.to_owned(),
        ..DumpProbe::default()
    };
    let read = tokio::time::timeout(timeout, read(ep, from, timeout, &mut out)).await;
    out.failure = match read {
        Ok(Ok(())) => None,
        Ok(Err(f)) => Some(f),
        Err(_) => Some(DumpFailure {
            kind: DumpFailureKind::Timeout,
            errno: None,
            detail: format!(
                "no format description and event within {} ms",
                timeout.as_millis()
            ),
            fix: "check the node's load and network, or raise cluster.probe_timeout_ms".to_owned(),
        }),
    };
    let took = start.elapsed();
    out.took_ms = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
    metrics::histogram!("sprig_cluster_probe_duration_seconds", "probe" => "binlog_dump")
        .record(took.as_secs_f64());
    if let Some(f) = &out.failure {
        metrics::counter!("sprig_cluster_probe_failures_total", "probe" => "binlog_dump")
            .increment(1);
        tracing::info!(node = %ep.node, addr = %ep.addr(), kind = ?f.kind, detail = %f.detail, "binlog dump probe failed");
    }
    out
}

async fn read(
    ep: &NodeEndpoint,
    from: &str,
    timeout: Duration,
    out: &mut DumpProbe,
) -> Result<(), DumpFailure> {
    let pos: GtidPos = from.parse().map_err(|e: SprigError| DumpFailure {
        kind: DumpFailureKind::Position,
        errno: None,
        detail: format!(
            "the node's gtid_binlog_pos {from:?} doesn't parse: {}",
            e.message
        ),
        fix: "check @@gtid_binlog_pos on the node".to_owned(),
    })?;
    let src = source(ep).await;
    let mut opts = StreamOptions::new(probe_server_id());
    opts.non_blocking = true;
    opts.connect_timeout = timeout;
    // The server sends nothing but the start events and the end; a
    // heartbeat would only arrive on a dump that hangs.
    opts.heartbeat = timeout;
    opts.silence_periods = 1;
    let mut stream = BinlogStream::connect(&src, &pos, &opts)
        .await
        .map_err(|e| classify(&e))?;
    let mut seen_fd = false;
    loop {
        let ev = stream.next().await.map_err(|e| classify(&e))?;
        if let Event::DumpEnd = ev {
            out.events.push("dump_end".to_owned());
            if seen_fd {
                return Ok(());
            }
            return Err(DumpFailure {
                kind: DumpFailureKind::Incomplete,
                errno: None,
                detail: "the dump ended before a format description".to_owned(),
                fix: "check the node's binlog files (SHOW BINARY LOGS)".to_owned(),
            });
        }
        out.events
            .push(event_type::name(stream.last_header().type_code).to_owned());
        match ev {
            Event::FormatDescription {
                server_version,
                checksums,
                ..
            } => {
                out.server_version = Some(server_version);
                out.checksums = Some(checksums);
                seen_fd = true;
            }
            Event::Incident(msg) => {
                return Err(DumpFailure {
                    kind: DumpFailureKind::Corrupt,
                    errno: None,
                    detail: format!("the binlog holds an incident event ({msg})"),
                    fix: "the server lost events: rotate the binlog (FLUSH BINARY LOGS) and \
                          re-seed consumers from a snapshot"
                        .to_owned(),
                });
            }
            _ => {}
        }
        if seen_fd && out.events.len() > MAX_EVENTS {
            return Ok(());
        }
    }
}

/// The binlog reader's view of an endpoint, TLS included.
async fn source(ep: &NodeEndpoint) -> BinlogSource {
    ep.binlog_source().await
}

/// A random id in the probe's range.
fn probe_server_id() -> u32 {
    #[allow(clippy::cast_possible_truncation)]
    let r = JobId::new_v7().0.as_u128() as u32;
    PROBE_SERVER_ID_BASE + r % PROBE_SERVER_ID_SPAN
}

/// Sorts a reader error into a failure kind with its fix.
#[must_use]
pub fn classify(e: &SprigError) -> DumpFailure {
    let msg = e.message.clone();
    let lower = msg.to_ascii_lowercase();
    let fail = |kind, errno, fix: &str| DumpFailure {
        kind,
        errno,
        detail: match errno {
            Some(n) => format!("error {n}: {msg}"),
            None => msg.clone(),
        },
        fix: fix.to_owned(),
    };
    match e.kind {
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => {
            if lower.contains("insecure transport") || lower.contains("secure transport") {
                return fail(
                    DumpFailureKind::Transport,
                    Some(errno),
                    "the reader connected without TLS: give the sprig_repl credential \
                     tls = \"verify\" and the cluster's ca_file",
                );
            }
            match errno {
                1045 => fail(
                    DumpFailureKind::Login,
                    Some(errno),
                    "check the sprig_repl credential (user, password, and REQUIRE clauses)",
                ),
                1227 | 1142 | 1044 => fail(
                    DumpFailureKind::Privilege,
                    Some(errno),
                    "GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl",
                ),
                1236 if lower.contains("binary log is not open") => fail(
                    DumpFailureKind::NoBinlog,
                    Some(errno),
                    "set log_bin in the server configuration and restart",
                ),
                1236 => fail(
                    DumpFailureKind::Position,
                    Some(errno),
                    "the server can't start a dump at its own position: check gtid_strict_mode \
                     = ON and the binlog files (SHOW BINARY LOGS)",
                ),
                _ => fail(
                    DumpFailureKind::Other,
                    Some(errno),
                    "see the server's error log",
                ),
            }
        }
        ErrorKind::Sprig(SprigCode::BinlogUnsupported) if msg.starts_with("TLS:") => fail(
            DumpFailureKind::Transport,
            None,
            "check the sprig_repl credential's tls and ca_file against the server's \
             certificate (ssl_cert, ssl_key, and the CA that signed them)",
        ),
        ErrorKind::Sprig(SprigCode::BinlogUnsupported) => {
            let fix = if lower.contains("authentication plugin") {
                "ALTER USER sprig_repl IDENTIFIED VIA mysql_native_password USING \
                 PASSWORD('...'): the binlog reader speaks no other plugin yet"
            } else if lower.contains("checksum") {
                "SET GLOBAL binlog_checksum = 'CRC32'"
            } else {
                "the binlog reader doesn't support this binlog; see the detail"
            };
            fail(DumpFailureKind::Unsupported, None, fix)
        }
        ErrorKind::Sprig(SprigCode::BinlogCorrupt) => fail(
            DumpFailureKind::Corrupt,
            None,
            "an event failed its checksum or didn't decode: check the binlog with \
             mariadb-binlog --verify-binlog-checksum",
        ),
        ErrorKind::Sprig(_) => fail(
            DumpFailureKind::Unreachable,
            None,
            "check that the node is up and reachable from the controller",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_reader_errors() {
        let e = SprigError::backend(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation",
        );
        let f = classify(&e);
        assert_eq!(f.kind, DumpFailureKind::Privilege);
        assert!(f.fix.starts_with("GRANT REPLICATION SLAVE"));
        assert!(f.kind.conclusive());

        let f = classify(&SprigError::backend(
            1236,
            *b"HY000",
            "Binary log is not open",
        ));
        assert_eq!(f.kind, DumpFailureKind::NoBinlog);
        let f = classify(&SprigError::backend(
            1236,
            *b"HY000",
            "Error: connecting slave requested to start from GTID 1-1-5, which is not in the master's binlog",
        ));
        assert_eq!(f.kind, DumpFailureKind::Position);
        let f = classify(&SprigError::backend(
            1045,
            *b"28000",
            "Access denied for user",
        ));
        assert_eq!(f.kind, DumpFailureKind::Login);
        let f = classify(&SprigError::new(
            SprigCode::BinlogUnsupported,
            "authentication plugin client_ed25519 is not supported by the binlog client",
        ));
        assert_eq!(f.kind, DumpFailureKind::Unsupported);
        assert!(f.fix.contains("mysql_native_password"));
        // Encrypted binlogs read over a dump, so nothing tells the operator
        // to turn encryption off; an unsupported event gets the generic fix.
        let f = classify(&SprigError::new(
            SprigCode::BinlogUnsupported,
            "encrypted binlog file; read it through a server, which decrypts",
        ));
        assert_eq!(f.kind, DumpFailureKind::Unsupported);
        assert!(!f.fix.contains("encrypt_binlog"), "{f:?}");
        let f = classify(&SprigError::new(
            SprigCode::BinlogDisconnected,
            "connecting to 10.0.0.1:3306 timed out",
        ));
        assert_eq!(f.kind, DumpFailureKind::Unreachable);
        assert!(!f.kind.conclusive());
    }

    #[test]
    fn a_failed_probe_is_a_blocking_gap_unless_inconclusive() {
        let mut p = DumpProbe::default();
        assert!(p.passed() && !p.blocks_feed() && p.gap().is_none());
        p.failure = Some(classify(&SprigError::backend(1227, *b"42000", "denied")));
        assert!(p.blocks_feed());
        let g = p.gap().unwrap();
        assert_eq!(g.severity, GapSeverity::Blocking);
        assert!(p.advisory().unwrap().contains("GRANT REPLICATION SLAVE"));
        p.failure.as_mut().unwrap().kind = DumpFailureKind::Timeout;
        assert!(!p.blocks_feed() && !p.passed());
    }

    #[test]
    fn probe_ids_stay_in_their_range() {
        for _ in 0..100 {
            let id = probe_server_id();
            assert!(
                (PROBE_SERVER_ID_BASE..PROBE_SERVER_ID_BASE + PROBE_SERVER_ID_SPAN).contains(&id)
            );
        }
    }

    #[tokio::test]
    async fn an_unreachable_node_is_inconclusive() {
        // Nothing listens on port 9 of localhost.
        let ep = NodeEndpoint::from_url(
            sprig_common::ids::NodeId::new("n1").unwrap(),
            "mysql://sprig_repl:pw@127.0.0.1:9",
        )
        .unwrap();
        let p = run(&ep, "", Duration::from_millis(500)).await;
        let f = p.failure.unwrap();
        assert!(
            matches!(
                f.kind,
                DumpFailureKind::Unreachable | DumpFailureKind::Timeout
            ),
            "{f:?}"
        );
    }
}
