//! A kind's policy at one subject: its mode, the level a new item goes to,
//! and the timers (escalation and auto-apply).
//!
//! Resolution goes through the six settings scopes like any setting; the
//! caller supplies a [`Lookup`] that resolves a key in the subject's
//! context (object, database, tenant, ancestors, cluster, DC, global).

use std::time::Duration;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Resolved, ScopeKind, Value};

use crate::model::{Kind, Level, Mode};
use crate::settings as keys;

/// Resolves a key in one subject's context.
pub trait Lookup {
    /// The resolved value and its source, `None` when the key is unknown.
    fn resolve(&self, key: &str) -> Option<Resolved>;
}

impl<F: Fn(&str) -> Option<Resolved>> Lookup for F {
    fn resolve(&self, key: &str) -> Option<Resolved> {
        self(key)
    }
}

/// A kind's policy at one subject.
#[derive(Debug, Clone, PartialEq)]
pub struct Policy {
    /// The mode as the gate sees it.
    pub mode: Mode,
    /// The level a new item goes to.
    pub level: Level,
    /// The scope the mode came from, as text.
    pub mode_source: String,
    /// `approvals.escalate_after` for the kind.
    pub escalate_after: Option<Duration>,
    /// `approvals.auto_apply_after` for the kind, unless forbidden.
    pub auto_apply_after: Option<Duration>,
    /// `approvals.allow_emergency`.
    pub allow_emergency: bool,
    /// `approvals.reraise_factor`.
    pub reraise_factor: f64,
    /// `approvals.ignore_once_days`.
    pub ignore_once_days: u32,
    /// Values that were set but couldn't be used, with why.
    pub warnings: Vec<String>,
}

impl Policy {
    /// The built-in policy for `mode` at `level`: no timers, the defaults.
    #[must_use]
    pub fn fixed(mode: Mode, level: Level) -> Self {
        Self {
            mode,
            level,
            mode_source: "default".into(),
            escalate_after: None,
            auto_apply_after: None,
            allow_emergency: keys::ALLOW_EMERGENCY.default,
            reraise_factor: keys::RERAISE_FACTOR.default,
            ignore_once_days: keys::IGNORE_ONCE_DAYS.default,
            warnings: Vec::new(),
        }
    }

    /// Resolves `kind`'s policy through `lookup`.
    #[must_use]
    pub fn resolve(kind: Kind, lookup: &dyn Lookup) -> Self {
        let (mode_text, source) = match lookup.resolve(kind.mode_key()) {
            Some(r) => (text(&r.value).unwrap_or_default(), r.source),
            None => (String::new(), ScopeKind::Default),
        };
        Self::resolve_with(kind, &mode_text, source, lookup)
    }

    /// As [`resolve`](Self::resolve), with the mode already resolved by the
    /// component (`mode_text` from `source`).
    #[must_use]
    pub fn resolve_with(
        kind: Kind,
        mode_text: &str,
        source: ScopeKind,
        lookup: &dyn Lookup,
    ) -> Self {
        let mut warnings = Vec::new();
        let mut mode = Mode::from_setting(mode_text);
        // Failover is held only when the platform says so (a safety
        // rule): anywhere else `approve` reads as automatic.
        if matches!(kind, Kind::FailoverCluster | Kind::FailoverDc)
            && mode == Mode::Approve
            && source != ScopeKind::Global
        {
            warnings.push(format!(
                "failover.mode = approve at the {source} scope is ignored: only the global scope may hold failovers"
            ));
            mode = Mode::On;
        }
        let start = lookup
            .resolve(keys::START_LEVEL.key)
            .and_then(|r| text(&r.value))
            .unwrap_or_else(|| keys::START_LEVEL.default.to_string());
        let level = level_for(kind, source, &start);
        let mut timer = |key: &str| -> Option<Duration> {
            let t = lookup.resolve(key).and_then(|r| text(&r.value))?;
            match TimerSpec::parse(&t) {
                Ok(spec) => spec.for_kind(kind),
                Err(e) => {
                    warnings.push(format!("{key}: {}", e.message));
                    None
                }
            }
        };
        let escalate_after = timer(keys::ESCALATE_AFTER.key);
        let mut auto_apply_after = timer(keys::AUTO_APPLY_AFTER.key);
        let forbidden = lookup
            .resolve(keys::AUTO_APPLY_FORBIDDEN.key)
            .and_then(|r| text(&r.value))
            .unwrap_or_default();
        if kind_listed(&forbidden, kind) {
            auto_apply_after = None;
        }
        let allow_emergency = lookup
            .resolve(keys::ALLOW_EMERGENCY.key)
            .and_then(|r| match r.value {
                Value::Bool(b) => Some(b),
                _ => None,
            })
            .unwrap_or(keys::ALLOW_EMERGENCY.default);
        let reraise_factor = lookup
            .resolve(keys::RERAISE_FACTOR.key)
            .and_then(|r| r.value.as_f64())
            .unwrap_or(keys::RERAISE_FACTOR.default);
        let ignore_once_days = lookup
            .resolve(keys::IGNORE_ONCE_DAYS.key)
            .and_then(|r| r.value.as_f64())
            .map_or(keys::IGNORE_ONCE_DAYS.default, |d| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let d = d.clamp(0.0, 3650.0) as u32;
                d
            });
        Self {
            mode,
            level,
            mode_source: source.to_string(),
            escalate_after,
            auto_apply_after,
            allow_emergency,
            reraise_factor,
            ignore_once_days,
            warnings,
        }
    }
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.clone()),
        _ => None,
    }
}

/// The level of a new item: the mode's source scope (or
/// `approvals.start_level`), raised to the kind's narrowest level.
#[must_use]
pub fn level_for(kind: Kind, source: ScopeKind, start_level: &str) -> Level {
    let resolved = match source {
        ScopeKind::Object => Level::Table,
        ScopeKind::Database => Level::Database,
        ScopeKind::Tenant | ScopeKind::Ancestor(_) => Level::Tenant,
        ScopeKind::Cluster | ScopeKind::Dc => Level::Dc,
        ScopeKind::Global | ScopeKind::Default => Level::Platform,
    };
    let chosen = match start_level {
        "resolved" | "" => resolved,
        other => Level::parse(other).unwrap_or(resolved),
    };
    let floor = kind.min_level();
    if floor == Level::Dc {
        // Cluster and DC kinds have no tenant levels: dc or platform.
        return if chosen == Level::Platform {
            Level::Platform
        } else {
            Level::Dc
        };
    }
    if chosen < floor { floor } else { chosen }
}

/// Whether a comma list of kinds and families (`index,dc.move`) names
/// `kind`. `*` names every kind.
#[must_use]
pub fn kind_listed(list: &str, kind: Kind) -> bool {
    list.split(',')
        .map(str::trim)
        .any(|k| !k.is_empty() && (k == "*" || k == kind.as_str() || k == kind.family()))
}

/// A timer value: `off`, a duration, or `kind=duration,...,*=duration`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimerSpec {
    /// `(kind or family or *, duration or off)`.
    pub entries: Vec<(String, Option<Duration>)>,
}

impl TimerSpec {
    /// Parses a timer value.
    ///
    /// # Errors
    /// 39977 `ER_SPRIG_APPROVAL_POLICY_INVALID` for text that doesn't parse.
    pub fn parse(s: &str) -> Result<Self> {
        let mut entries = Vec::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (k, v) = match part.split_once('=') {
                Some((k, v)) => (k.trim(), v.trim()),
                None => ("*", part),
            };
            if k != "*" && Kind::ALL.iter().all(|x| x.as_str() != k && x.family() != k) {
                return Err(invalid(format!("unknown kind or family {k:?} in {s:?}")));
            }
            entries.push((k.to_owned(), parse_duration(v)?));
        }
        Ok(Self { entries })
    }

    /// The timer for `kind`: its own entry, then its family's, then `*`.
    #[must_use]
    pub fn for_kind(&self, kind: Kind) -> Option<Duration> {
        let find = |k: &str| self.entries.iter().find(|(x, _)| x == k).map(|(_, d)| *d);
        find(kind.as_str())
            .or_else(|| find(kind.family()))
            .or_else(|| find("*"))
            .flatten()
    }
}

/// `off`, `0`, or a number with `s`, `m`, `h` or `d`.
///
/// # Errors
/// 39977 for anything else.
pub fn parse_duration(v: &str) -> Result<Option<Duration>> {
    let v = v.trim();
    if v.eq_ignore_ascii_case("off") || v == "0" || v.is_empty() {
        return Ok(None);
    }
    let split = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    let (n, unit) = v.split_at(split);
    let n: u64 = n
        .parse()
        .map_err(|_| invalid(format!("{v:?} is not a duration (90m, 24h, 3d or off)")))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" | "" => n * 3600,
        "d" => n * 86_400,
        _ => return Err(invalid(format!("{v:?} has an unknown unit (s, m, h or d)"))),
    };
    if secs == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs(secs)))
}

fn invalid(m: String) -> SprigError {
    SprigError::new(SprigCode::ApprovalPolicyInvalid, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("off").unwrap(), None);
        assert_eq!(
            parse_duration("90m").unwrap(),
            Some(Duration::from_secs(5400))
        );
        assert_eq!(
            parse_duration("24h").unwrap(),
            Some(Duration::from_secs(86_400))
        );
        assert_eq!(
            parse_duration("3d").unwrap(),
            Some(Duration::from_secs(259_200))
        );
        assert_eq!(
            parse_duration("30s").unwrap(),
            Some(Duration::from_secs(30))
        );
        assert!(parse_duration("3w").is_err());
        assert!(parse_duration("soon").is_err());
    }

    #[test]
    fn timer_lists_pick_the_most_specific_entry() {
        let t = TimerSpec::parse("dc.move=off,index=24h,*=72h").unwrap();
        assert_eq!(t.for_kind(Kind::DcMove), None);
        assert_eq!(
            t.for_kind(Kind::IndexDrop),
            Some(Duration::from_secs(86_400))
        );
        assert_eq!(
            t.for_kind(Kind::ShardSplit),
            Some(Duration::from_secs(259_200))
        );
        let one = TimerSpec::parse("2h").unwrap();
        assert_eq!(one.for_kind(Kind::DcMove), Some(Duration::from_secs(7200)));
        assert!(TimerSpec::parse("bogus=2h").is_err());
        assert_eq!(TimerSpec::parse("").unwrap().for_kind(Kind::DcMove), None);
    }

    #[test]
    fn levels_follow_the_source_and_the_kind_floor() {
        assert_eq!(
            level_for(Kind::IndexCreate, ScopeKind::Object, "resolved"),
            Level::Table
        );
        assert_eq!(
            level_for(Kind::IndexCreate, ScopeKind::Global, "resolved"),
            Level::Platform
        );
        assert_eq!(
            level_for(Kind::IndexCreate, ScopeKind::Global, "tenant"),
            Level::Tenant
        );
        assert_eq!(
            level_for(Kind::DcMove, ScopeKind::Object, "resolved"),
            Level::Tenant
        );
        assert_eq!(
            level_for(Kind::DcMove, ScopeKind::Dc, "resolved"),
            Level::Dc
        );
        assert_eq!(
            level_for(Kind::ShardSplit, ScopeKind::Tenant, "table"),
            Level::Database
        );
        assert_eq!(
            level_for(Kind::FailoverCluster, ScopeKind::Global, "resolved"),
            Level::Platform
        );
        assert_eq!(
            level_for(Kind::FailoverCluster, ScopeKind::Global, "tenant"),
            Level::Dc
        );
        assert_eq!(
            level_for(
                Kind::DcMove,
                ScopeKind::Ancestor(sprig_common::ids::TenantId(1)),
                "resolved"
            ),
            Level::Tenant
        );
    }

    #[test]
    fn kind_lists() {
        assert!(kind_listed("index, dc.move", Kind::DcMove));
        assert!(kind_listed("index", Kind::IndexDrop));
        assert!(!kind_listed("index", Kind::ShardMove));
        assert!(kind_listed("*", Kind::ShardMove));
        assert!(!kind_listed("", Kind::ShardMove));
    }
}
