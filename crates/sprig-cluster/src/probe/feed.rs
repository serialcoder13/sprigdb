//! The binlog settings the change feed and the mover need (chapter 09,
//! "Binlog for the change feed"; chapter 38, "Binlog requirements").
//!
//! [`check`] reads only server variables. Whether a dump actually streams
//! valid events is the binlog dump probe's job ([`super::dump`], kept in
//! [`ActiveProbes::binlog_dump`](super::ActiveProbes)); [`check_with_dump`]
//! folds its result in, so a node whose settings pass but whose binlog
//! `sprig_repl` can't read (no `REPLICATION SLAVE`, a checksum or format
//! the reader doesn't know, corrupt events) is `unsupported` too.

use serde::{Deserialize, Serialize};

use super::NodeProbe;
use super::dump::DumpProbe;
use super::parse::{BinlogFormat, NodeFacts, RowImage, RowMetadata};

/// The default of `feed.retention_hours` (chapter 38, which owns the key):
/// how long binlogs should stay on the server.
pub const DEFAULT_RETENTION_HOURS: u64 = 168;

/// What the change feed can do with a node's binlog (chapter 38).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FeedLevel {
    /// A required setting is missing: no feed from here (39802).
    #[default]
    Unsupported,
    /// Only the annotate settings are missing: one-second stamps from the
    /// event header, and no `replace` events.
    Degraded,
    /// Every requirement holds.
    Full,
}

/// How much a gap matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapSeverity {
    /// The node can't be a mover or feed source.
    Blocking,
    /// The feed works in its degraded form.
    Degrading,
    /// Recommended; nothing is disabled.
    Advisory,
}

/// One unmet requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedGap {
    /// The server variable.
    pub variable: String,
    /// What is needed.
    pub needed: String,
    /// What the server has.
    pub found: String,
    /// How much it matters.
    pub severity: GapSeverity,
    /// The statement or configuration change that fixes it.
    pub fix: String,
}

/// The result of [`check`] on one node.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FeedCheck {
    /// What the feed can do here.
    pub level: FeedLevel,
    /// Every unmet requirement, blocking first.
    pub gaps: Vec<FeedGap>,
}

impl FeedCheck {
    /// Whether the mover and the feed can read this node's binlog (the
    /// settings half of `mover_source`).
    #[must_use]
    pub fn source_ok(&self) -> bool {
        self.level != FeedLevel::Unsupported
    }
}

fn on_off(b: bool) -> String {
    if b { "ON" } else { "OFF" }.to_owned()
}

fn text<T: Serialize>(v: Option<T>) -> String {
    match v.map(|v| serde_json::to_value(v)) {
        Some(Ok(serde_json::Value::String(s))) => s,
        _ => "unknown".to_owned(),
    }
}

/// Checks one node's binlog settings against the feed's requirements.
/// Replicas and Galera nodes must also log what they apply
/// (`log_slave_updates`, `replicate_annotate_row_events`).
///
/// `retention_hours` is `feed.retention_hours`; a shorter
/// `binlog_expire_logs_seconds` is advisory, because older history comes
/// from the archive.
#[must_use]
#[allow(clippy::too_many_lines)] // one block per requirement, like the chapter's table
pub fn check(facts: &NodeFacts, retention_hours: u64) -> FeedCheck {
    let b = &facts.binlog;
    let applies_others = facts.is_replica() || facts.galera.is_some();
    let mut gaps = Vec::new();
    let mut gap = |variable: &str, needed: &str, found: String, severity, fix: String| {
        gaps.push(FeedGap {
            variable: variable.to_owned(),
            needed: needed.to_owned(),
            found,
            severity,
            fix,
        });
    };
    if !b.log_bin {
        gap(
            "log_bin",
            "ON",
            on_off(false),
            GapSeverity::Blocking,
            "set log_bin in the server configuration and restart".to_owned(),
        );
    }
    if b.format != Some(BinlogFormat::Row) {
        gap(
            "binlog_format",
            "ROW",
            text(b.format),
            GapSeverity::Blocking,
            "SET GLOBAL binlog_format = 'ROW'".to_owned(),
        );
    }
    if b.row_image != Some(RowImage::Full) {
        gap(
            "binlog_row_image",
            "FULL",
            text(b.row_image),
            GapSeverity::Blocking,
            "SET GLOBAL binlog_row_image = 'FULL'".to_owned(),
        );
    }
    if !b.gtid_strict_mode {
        gap(
            "gtid_strict_mode",
            "ON",
            on_off(false),
            GapSeverity::Blocking,
            "SET GLOBAL gtid_strict_mode = ON".to_owned(),
        );
    }
    if applies_others && !b.log_slave_updates {
        gap(
            "log_slave_updates",
            "ON",
            on_off(false),
            GapSeverity::Blocking,
            "set log_slave_updates in the server configuration and restart".to_owned(),
        );
    }
    // Absent means the server didn't report it; both default to ON.
    if b.annotate_row_events == Some(false) {
        gap(
            "binlog_annotate_row_events",
            "ON",
            on_off(false),
            GapSeverity::Degrading,
            "SET GLOBAL binlog_annotate_row_events = ON".to_owned(),
        );
    }
    if applies_others && b.replicate_annotate_row_events == Some(false) {
        gap(
            "replicate_annotate_row_events",
            "ON",
            on_off(false),
            GapSeverity::Degrading,
            "set replicate_annotate_row_events in the server configuration and restart".to_owned(),
        );
    }
    if b.row_metadata != Some(RowMetadata::Full) {
        gap(
            "binlog_row_metadata",
            "FULL (recommended)",
            text(b.row_metadata),
            GapSeverity::Advisory,
            "SET GLOBAL binlog_row_metadata = 'FULL'".to_owned(),
        );
    }
    let wanted = retention_hours.saturating_mul(3600);
    if b.expire_logs_seconds != 0 && b.expire_logs_seconds < wanted {
        gap(
            "binlog_expire_logs_seconds",
            &format!("at least {wanted}"),
            b.expire_logs_seconds.to_string(),
            GapSeverity::Advisory,
            format!("SET GLOBAL binlog_expire_logs_seconds = {wanted}"),
        );
    }
    let level = if gaps.iter().any(|g| g.severity == GapSeverity::Blocking) {
        FeedLevel::Unsupported
    } else if gaps.iter().any(|g| g.severity == GapSeverity::Degrading) {
        FeedLevel::Degraded
    } else {
        FeedLevel::Full
    };
    gaps.sort_by_key(|g| g.severity as u8);
    FeedCheck { level, gaps }
}

/// [`check`], plus the dump probe's result where it ran: a conclusive
/// failure ([`DumpProbe::blocks_feed`]) is a blocking gap, so the level is
/// `unsupported`. An inconclusive one (a timeout, a lost connection) only
/// adds an advisory gap.
#[must_use]
pub fn check_with_dump(
    facts: &NodeFacts,
    retention_hours: u64,
    dump: Option<&DumpProbe>,
) -> FeedCheck {
    let mut c = check(facts, retention_hours);
    if let Some(mut g) = dump.and_then(DumpProbe::gap) {
        if dump.is_some_and(DumpProbe::blocks_feed) {
            c.level = FeedLevel::Unsupported;
        } else {
            g.severity = GapSeverity::Advisory;
        }
        c.gaps.push(g);
        c.gaps.sort_by_key(|g| g.severity as u8);
    }
    c
}

/// One line per gap on every answering node, each with its fix, for the
/// registration report and the decision log; the dump probe's gap on the
/// node it ran on.
#[must_use]
pub fn report(probes: &[NodeProbe], retention_hours: u64) -> Vec<String> {
    probes
        .iter()
        .filter_map(|p| {
            p.facts().map(|f| {
                let dump = p.active.as_ref().and_then(|a| a.binlog_dump.as_ref());
                (p, check_with_dump(f, retention_hours, dump))
            })
        })
        .flat_map(|(p, c)| {
            c.gaps.into_iter().map(move |g| {
                format!(
                    "node {}: {} is {}, needs {} ({:?}): {}",
                    p.node, g.variable, g.found, g.needed, g.severity, g.fix
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{facts_mut, galera, raw, replication};

    #[test]
    fn captured_nodes_are_full_sources() {
        for name in [
            "11.8-single",
            "11.8-primary",
            "11.8-replica",
            "11.8-galera",
            "12.3-single",
        ] {
            let f = NodeFacts::parse(&raw(name));
            let c = check(&f, DEFAULT_RETENTION_HOURS);
            assert_eq!(c.level, FeedLevel::Full, "{name}: {:?}", c.gaps);
            assert!(c.gaps.is_empty(), "{name}: {:?}", c.gaps);
        }
    }

    #[test]
    fn minimal_row_image_blocks() {
        let mut p = replication();
        facts_mut(&mut p[0]).binlog.row_image = Some(RowImage::Minimal);
        let c = check(p[0].facts().unwrap(), DEFAULT_RETENTION_HOURS);
        assert_eq!(c.level, FeedLevel::Unsupported);
        assert_eq!(c.gaps[0].variable, "binlog_row_image");
        assert_eq!(c.gaps[0].found, "MINIMAL");
        assert_eq!(c.gaps[0].fix, "SET GLOBAL binlog_row_image = 'FULL'");
    }

    #[test]
    fn replicas_and_galera_nodes_must_log_what_they_apply() {
        let mut p = replication();
        facts_mut(&mut p[1]).binlog.log_slave_updates = false;
        facts_mut(&mut p[0]).binlog.log_slave_updates = false;
        // Off on the primary doesn't matter.
        assert_eq!(check(p[0].facts().unwrap(), 168).level, FeedLevel::Full);
        assert_eq!(
            check(p[1].facts().unwrap(), 168).level,
            FeedLevel::Unsupported
        );
        let mut g = galera();
        facts_mut(&mut g[2]).binlog.log_slave_updates = false;
        assert_eq!(
            check(g[2].facts().unwrap(), 168).level,
            FeedLevel::Unsupported
        );
    }

    #[test]
    fn missing_annotations_degrade() {
        let mut p = replication();
        facts_mut(&mut p[1]).binlog.replicate_annotate_row_events = Some(false);
        facts_mut(&mut p[0]).binlog.annotate_row_events = Some(false);
        // replicate_annotate_row_events only matters where others' events are logged.
        facts_mut(&mut p[0]).binlog.replicate_annotate_row_events = Some(false);
        let c = check(p[0].facts().unwrap(), 168);
        assert_eq!(c.level, FeedLevel::Degraded);
        assert_eq!(c.gaps.len(), 1, "{:?}", c.gaps);
        assert_eq!(check(p[1].facts().unwrap(), 168).level, FeedLevel::Degraded);
    }

    #[test]
    fn retention_and_metadata_are_advisory() {
        let mut p = replication();
        let b = &mut facts_mut(&mut p[0]).binlog;
        b.expire_logs_seconds = 86_400;
        b.row_metadata = Some(RowMetadata::Minimal);
        let c = check(p[0].facts().unwrap(), 168);
        assert_eq!(c.level, FeedLevel::Full);
        assert!(c.source_ok());
        let vars: Vec<_> = c.gaps.iter().map(|g| g.variable.as_str()).collect();
        assert_eq!(vars, ["binlog_row_metadata", "binlog_expire_logs_seconds"]);
        assert!(c.gaps[1].fix.ends_with("= 604800"));
        // 0 means never expire.
        facts_mut(&mut p[0]).binlog.expire_logs_seconds = 0;
        assert_eq!(check(p[0].facts().unwrap(), 168).gaps.len(), 1);
        let lines = report(&p, 168);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with("node n1: binlog_row_metadata is MINIMAL"));
    }

    #[test]
    fn a_failed_dump_probe_blocks_and_an_inconclusive_one_advises() {
        use crate::probe::dump::{DumpFailureKind, classify};
        let mut p = replication();
        let mut dump = DumpProbe {
            failure: Some(classify(&sprig_common::SprigError::backend(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s)",
            ))),
            ..DumpProbe::default()
        };
        let f = p[0].facts().unwrap().clone();
        let c = check_with_dump(&f, 168, Some(&dump));
        assert_eq!(c.level, FeedLevel::Unsupported);
        assert_eq!(c.gaps[0].variable, "binlog dump");
        assert!(c.gaps[0].fix.starts_with("GRANT REPLICATION SLAVE"));
        p[0].active = Some(crate::testutil::all_passed());
        p[0].active.as_mut().unwrap().binlog_dump = Some(dump.clone());
        let lines = report(&p, 168);
        assert!(lines[0].contains("binlog dump"), "{lines:?}");

        dump.failure.as_mut().unwrap().kind = DumpFailureKind::Timeout;
        let c = check_with_dump(&f, 168, Some(&dump));
        assert_eq!(c.level, FeedLevel::Full);
        assert_eq!(c.gaps[0].severity, GapSeverity::Advisory);
        assert_eq!(
            check_with_dump(&f, 168, Some(&DumpProbe::default())),
            check(&f, 168)
        );
    }

    #[test]
    fn statement_format_and_no_binlog_block() {
        let mut p = replication();
        let b = &mut facts_mut(&mut p[0]).binlog;
        b.log_bin = false;
        b.format = Some(BinlogFormat::Mixed);
        b.gtid_strict_mode = false;
        let c = check(p[0].facts().unwrap(), 168);
        assert_eq!(c.level, FeedLevel::Unsupported);
        let blocking: Vec<_> = c
            .gaps
            .iter()
            .filter(|g| g.severity == GapSeverity::Blocking)
            .map(|g| g.variable.as_str())
            .collect();
        assert_eq!(blocking, ["log_bin", "binlog_format", "gtid_strict_mode"]);
    }
}
