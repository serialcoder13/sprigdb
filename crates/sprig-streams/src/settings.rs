//! The `stream.*` keys this crate reads (chapter 45, "Configuration").
//!
//! Registered here: the keys the core uses, and `stream.produce_wait_ms`
//! (Kafka's produce). `stream.change_snapshot` (no snapshots yet) and
//! `stream.sequencer_max_restarts` belong to steps not built yet and are
//! not registered; the `kafka.*` and `listener.kafka.*` keys are the
//! gateway's.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// Tenant, database and global.
const TENANT_DB: Scopes = Scopes(0b10_0110);
/// Object, database and tenant.
const OBJECT_DB_TENANT: Scopes = Scopes(0b00_0111);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 45,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `stream.partitions_default`: partitions of a new stream.
    PARTITIONS_DEFAULT: u64 = "stream.partitions_default", 12, TENANT_DB,
    HardBounds::range(1, 10_000),
    "Partitions of a new stream"
);
key!(
    /// `stream.retention_hours`: time retention.
    RETENTION_HOURS: u64 = "stream.retention_hours", 168, Scopes::DATA,
    HardBounds::range(1, 87_600),
    "Time retention of a stream's records"
);
key!(
    /// `stream.retention_bytes`: size retention per partition; 0 is none.
    RETENTION_BYTES: u64 = "stream.retention_bytes", 0, Scopes::DATA,
    HardBounds::range(0, i64::MAX),
    "Size retention per partition; 0 = none"
);
key!(
    /// `stream.max_message_bytes`: the largest record.
    MAX_MESSAGE_BYTES: u64 = "stream.max_message_bytes", 1_048_576, Scopes::DATA,
    HardBounds::range(1, 1_073_741_824),
    "Largest record of a stream"
);
key!(
    /// `stream.compaction_lag_minutes`: the minimum age before compaction.
    COMPACTION_LAG_MINUTES: u64 = "stream.compaction_lag_minutes", 60, Scopes::DATA,
    HardBounds::range(0, 525_600),
    "Minimum age of a record before compaction"
);
key!(
    /// `stream.tombstone_hours`: how long tombstones stay on compacted
    /// streams.
    TOMBSTONE_HOURS: u64 = "stream.tombstone_hours", 24, Scopes::DATA,
    HardBounds::range(0, 87_600),
    "How long tombstones stay on compacted streams"
);
key!(
    /// `stream.change_topic`: keep a change topic for the object.
    CHANGE_TOPIC: bool = "stream.change_topic", false, OBJECT_DB_TENANT, HardBounds::NONE,
    "Keep a change topic for the object"
);
key!(
    /// `stream.sequencer_batch_ms`: the batching window of a sequencer.
    SEQUENCER_BATCH_MS: u64 = "stream.sequencer_batch_ms", 10, Scopes::PROCESS,
    HardBounds::range(0, 10_000),
    "Batching window of a stream sequencer"
);

/// `stream.produce_wait_ms`: how long a Kafka produce waits for its
/// records' offsets (chapter 45, "Produce").
pub static PRODUCE_WAIT_MS: Setting<u64> = Setting {
    key: "stream.produce_wait_ms",
    default: 2000,
    scopes: Scopes(0b10_0100),
    hard: HardBounds::range(0, 600_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 45,
    doc: "How long a Kafka produce waits for its records' offsets",
};
sprig_settings::register!(PRODUCE_WAIT_MS);

/// The settings `ALTER STREAM ... SET (...)` sets on one stream (object
/// scope).
pub const STREAM_SCOPED: &[&Setting<u64>] = &[
    &RETENTION_HOURS,
    &RETENTION_BYTES,
    &MAX_MESSAGE_BYTES,
    &COMPACTION_LAG_MINUTES,
    &TOMBSTONE_HOURS,
];

/// One stream-scoped setting of `ALTER STREAM ... SET | RESET`: its key and
/// value (`None`: reset to the inherited value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingChange {
    /// The setting.
    pub key: &'static str,
    /// The new value, or `None` to reset.
    pub value: Option<u64>,
}

/// Checks `name = value` for a stream: a key of [`STREAM_SCOPED`] and a
/// whole number inside its hard bounds.
///
/// # Errors
/// The text of a refusal (the caller wraps it).
pub fn stream_setting(name: &str, value: Option<&str>) -> Result<SettingChange, String> {
    let s = STREAM_SCOPED
        .iter()
        .find(|s| s.key == name)
        .ok_or_else(|| {
            format!(
                "'{name}' can't be set on a stream; stream settings are {}",
                STREAM_SCOPED
                    .iter()
                    .map(|s| s.key)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    let Some(v) = value else {
        return Ok(SettingChange {
            key: s.key,
            value: None,
        });
    };
    let n: u64 = v
        .trim()
        .parse()
        .map_err(|_| format!("'{v}' is not a whole number for {name}"))?;
    #[allow(clippy::cast_precision_loss)]
    let f = n as f64;
    if s.hard.min.is_some_and(|m| f < m) || s.hard.max.is_some_and(|m| f > m) {
        return Err(format!(
            "{n} is outside {name}'s bounds ({} to {})",
            s.hard.min.unwrap_or(0.0),
            s.hard.max.unwrap_or(f64::MAX)
        ));
    }
    Ok(SettingChange {
        key: s.key,
        value: Some(n),
    })
}

/// Retention and compaction settings of one stream, resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// `stream.retention_hours`.
    pub hours: u64,
    /// `stream.retention_bytes` (per partition; 0 is none).
    pub bytes: u64,
    /// `stream.compaction_lag_minutes`.
    pub compaction_lag_minutes: u64,
    /// `stream.tombstone_hours`.
    pub tombstone_hours: u64,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            hours: RETENTION_HOURS.default,
            bytes: RETENTION_BYTES.default,
            compaction_lag_minutes: COMPACTION_LAG_MINUTES.default,
            tombstone_hours: TOMBSTONE_HOURS.default,
        }
    }
}

/// `'7 days'`, `'72 hours'`, `'90 minutes'` (rounded up to whole hours),
/// or a bare number of hours, as `CREATE STREAM ... RETENTION` takes it.
#[must_use]
pub fn parse_retention(text: &str) -> Option<u64> {
    let t = text.trim().to_ascii_lowercase();
    let mut parts = t.split_ascii_whitespace();
    let n: u64 = parts.next()?.parse().ok()?;
    let hours = match parts.next() {
        None | Some("h" | "hour" | "hours") => n,
        Some("d" | "day" | "days") => n.checked_mul(24)?,
        Some("w" | "week" | "weeks") => n.checked_mul(168)?,
        Some("m" | "min" | "minute" | "minutes") => n.div_ceil(60),
        Some("ms") => n.div_ceil(3_600_000),
        _ => return None,
    };
    if parts.next().is_some() || hours == 0 {
        return None;
    }
    Some(hours)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_settings() {
        assert_eq!(
            stream_setting("stream.retention_bytes", Some("1000")),
            Ok(SettingChange {
                key: "stream.retention_bytes",
                value: Some(1000)
            })
        );
        assert_eq!(
            stream_setting("stream.max_message_bytes", None)
                .unwrap()
                .value,
            None
        );
        assert!(stream_setting("stream.max_message_bytes", Some("0")).is_err());
        assert!(stream_setting("stream.partitions_default", Some("3")).is_err());
        assert!(stream_setting("stream.retention_hours", Some("x")).is_err());
    }

    #[test]
    fn retention_text() {
        assert_eq!(parse_retention("7 days"), Some(168));
        assert_eq!(parse_retention("72 hours"), Some(72));
        assert_eq!(parse_retention("90 minutes"), Some(2));
        assert_eq!(parse_retention("12"), Some(12));
        assert_eq!(parse_retention("0 days"), None);
        assert_eq!(parse_retention("7 fortnights"), None);
    }
}
