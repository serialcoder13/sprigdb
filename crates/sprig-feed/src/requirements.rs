//! Binlog requirements and `feed_source` (chapter 38, "Binlog
//! requirements"; build step 1).

use std::collections::HashMap;

use mysql_async::prelude::Queryable;
use sprig_common::{Result, SprigCode, SprigError};

/// What a cluster's binlog settings allow the feed to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedSource {
    /// Every requirement holds.
    Full,
    /// Only the annotate settings are missing: one-second stamps from
    /// event headers, no `replace` hints.
    Degraded(Vec<String>),
    /// The feed can't read this cluster (39802).
    Unsupported(Vec<String>),
}

impl FeedSource {
    /// 39802 for an unsupported source.
    ///
    /// # Errors
    /// 39802 naming what fails.
    pub fn require(&self) -> Result<()> {
        match self {
            FeedSource::Unsupported(why) => Err(SprigError::new(
                SprigCode::FeedSourceUnsupported,
                format!(
                    "binlog settings fail the feed's requirements: {}",
                    why.join("; ")
                ),
            )),
            _ => Ok(()),
        }
    }
}

/// The variables [`judge`] reads.
pub const VARIABLES: &[&str] = &[
    "log_bin",
    "binlog_format",
    "binlog_row_image",
    "binlog_row_metadata",
    "binlog_annotate_row_events",
    "gtid_strict_mode",
    "log_slave_updates",
    "replicate_annotate_row_events",
    "binlog_expire_logs_seconds",
];

fn on(v: Option<&String>) -> bool {
    v.is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("ON"))
}

/// Judges a node's variables (names lower-case). `replica` is true when
/// the feed reads a replica or a Galera node, which must keep events and
/// annotations; `retention_hours` is `feed.retention_hours`.
#[must_use]
pub fn judge<S: std::hash::BuildHasher>(
    vars: &HashMap<String, String, S>,
    replica: bool,
    retention_hours: u64,
) -> FeedSource {
    let get = |k: &str| vars.get(k);
    let mut hard = Vec::new();
    let mut soft = Vec::new();
    if !on(get("log_bin")) {
        hard.push("log_bin is OFF".to_owned());
    }
    if !get("binlog_format").is_some_and(|v| v.eq_ignore_ascii_case("ROW")) {
        hard.push("binlog_format isn't ROW".to_owned());
    }
    if !get("binlog_row_image").is_some_and(|v| v.eq_ignore_ascii_case("FULL")) {
        hard.push("binlog_row_image isn't FULL".to_owned());
    }
    if !on(get("gtid_strict_mode")) {
        hard.push("gtid_strict_mode is OFF".to_owned());
    }
    if replica && !on(get("log_slave_updates")) {
        hard.push("log_slave_updates is OFF on a replica".to_owned());
    }
    if !on(get("binlog_annotate_row_events")) {
        soft.push("binlog_annotate_row_events is OFF".to_owned());
    }
    if replica && !on(get("replicate_annotate_row_events")) {
        soft.push("replicate_annotate_row_events is OFF on a replica".to_owned());
    }
    let expire: u64 = get("binlog_expire_logs_seconds")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    // 0 keeps binlogs forever.
    if expire != 0 && expire < retention_hours * 3600 {
        soft.push(format!(
            "binlog_expire_logs_seconds = {expire} keeps less than feed.retention_hours"
        ));
    }
    if !hard.is_empty() {
        hard.extend(soft);
        FeedSource::Unsupported(hard)
    } else if soft.is_empty() {
        FeedSource::Full
    } else {
        FeedSource::Degraded(soft)
    }
}

/// Reads [`VARIABLES`] from a node.
///
/// # Errors
/// The connection's errors.
pub async fn read_variables(c: &mut mysql_async::Conn) -> Result<HashMap<String, String>> {
    let list = VARIABLES
        .iter()
        .map(|v| format!("'{v}'"))
        .collect::<Vec<_>>()
        .join(",");
    let rows: Vec<(String, String)> = c
        .query(format!(
            "SELECT LOWER(VARIABLE_NAME), VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES \
             WHERE LOWER(VARIABLE_NAME) IN ({list})"
        ))
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("read binlog settings: {e}"),
            )
        })?;
    Ok(rows.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(over: &[(&str, &str)]) -> HashMap<String, String> {
        let mut m: HashMap<String, String> = [
            ("log_bin", "ON"),
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "FULL"),
            ("binlog_annotate_row_events", "ON"),
            ("gtid_strict_mode", "ON"),
            ("log_slave_updates", "ON"),
            ("replicate_annotate_row_events", "ON"),
            ("binlog_expire_logs_seconds", "0"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
        for (k, v) in over {
            m.insert((*k).to_owned(), (*v).to_owned());
        }
        m
    }

    #[test]
    fn judges_settings() {
        assert_eq!(judge(&vars(&[]), true, 168), FeedSource::Full);
        assert!(matches!(
            judge(&vars(&[("binlog_annotate_row_events", "OFF")]), false, 168),
            FeedSource::Degraded(_)
        ));
        assert!(matches!(
            judge(&vars(&[("binlog_expire_logs_seconds", "3600")]), false, 168),
            FeedSource::Degraded(_)
        ));
        let u = judge(&vars(&[("binlog_format", "MIXED")]), false, 168);
        assert!(u.require().is_err());
        assert!(matches!(
            judge(&vars(&[("log_slave_updates", "OFF")]), true, 168),
            FeedSource::Unsupported(_)
        ));
        assert_eq!(
            judge(&vars(&[("log_slave_updates", "OFF")]), false, 168),
            FeedSource::Full
        );
    }
}
