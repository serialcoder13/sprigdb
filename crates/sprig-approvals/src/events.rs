//! The one JSON shape webhooks, streams and emails carry (chapter 52,
//! "Events").

use serde::{Deserialize, Serialize};

use crate::gate::Event;
use crate::model::Micros;

/// An event as sent outside.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Outbound {
    /// `recommendation.new`, ...
    pub event: String,
    /// The item's id.
    pub id: String,
    /// The tenant's id, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<u64>,
    /// The tenant's name, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The kind.
    pub kind: String,
    /// The level.
    pub level: String,
    /// Who is notified now.
    pub notify_level: String,
    /// The state after the event.
    pub state: String,
    /// What it is about, for people.
    pub target: String,
    /// One line.
    pub summary: String,
    /// The statement or command.
    pub action: String,
    /// When it auto-applies, RFC 3339.
    pub auto_apply_at: Option<String>,
    /// Applied as an emergency.
    pub emergency: bool,
    /// Who caused the event.
    pub actor: String,
    /// A link to Studio, when `approvals.studio_url` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// When, RFC 3339.
    pub at: String,
}

impl Outbound {
    /// The outbound form of `ev`; `studio` is `approvals.studio_url`.
    #[must_use]
    pub fn of(ev: &Event, studio: &str) -> Self {
        let r = &ev.rec;
        let link = (!studio.is_empty()).then(|| {
            let t = r.subject.tenant_name.clone().unwrap_or_else(|| {
                r.subject
                    .tenant
                    .map_or_else(|| "_".into(), |t| t.to_string())
            });
            format!(
                "{}/studio/t/{t}/inbox/{}",
                studio.trim_end_matches('/'),
                r.id
            )
        });
        Self {
            event: ev.kind.name().to_owned(),
            id: r.id.to_string(),
            tenant_id: r.subject.tenant.map(|t| t.0),
            tenant: r.subject.tenant_name.clone(),
            kind: r.kind.as_str().to_owned(),
            level: r.level.as_str().to_owned(),
            notify_level: r.notify_level.to_string(),
            state: r.state.as_str().to_owned(),
            target: r.subject.display(),
            summary: r.summary.clone(),
            action: r.action.text.clone(),
            auto_apply_at: r.auto_apply_at_us.map(rfc3339),
            emergency: r.emergency,
            actor: ev.actor.clone(),
            link,
            at: rfc3339(ev.at_us),
        }
    }
}

/// `2026-11-10T14:03:11Z` from microseconds since the epoch.
#[must_use]
pub fn rfc3339(us: Micros) -> String {
    let secs = us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to (year, month, day), Howard Hinnant's algorithm.
fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parses `2026-11-10T14:03:11Z` (or with `+00:00`, fractions ignored) to
/// microseconds.
#[must_use]
pub fn parse_rfc3339(s: &str) -> Option<Micros> {
    let s = s.trim();
    let (date, time) = s.split_once(['T', ' '])?;
    let mut dp = date.split('-').map(str::parse::<i64>);
    let (y, m, d) = (dp.next()?.ok()?, dp.next()?.ok()?, dp.next()?.ok()?);
    let time = time.trim_end_matches('Z').trim_end_matches("+00:00");
    let time = time.split('.').next()?;
    let mut tp = time.split(':').map(str::parse::<i64>);
    let (hh, mm) = (tp.next()?.ok()?, tp.next()?.ok()?);
    let ss = tp.next().and_then(Result::ok).unwrap_or(0);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Days from civil, inverse of `civil`.
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400) + hh * 3600 + mm * 60 + ss) * 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_round_trip() {
        let us = 1_794_319_391_000_000; // 2026-11-10T14:03:11Z
        assert_eq!(rfc3339(us), "2026-11-10T14:03:11Z");
        assert_eq!(parse_rfc3339("2026-11-10T14:03:11Z"), Some(us));
        assert_eq!(parse_rfc3339("2026-11-10T14:03:11.5+00:00"), Some(us));
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            parse_rfc3339("2024-02-29T00:00:00Z")
                .map(rfc3339)
                .as_deref(),
            Some("2024-02-29T00:00:00Z")
        );
        assert_eq!(parse_rfc3339("nope"), None);
    }
}
