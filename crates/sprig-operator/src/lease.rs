//! The operator's leader election through a `coordination.k8s.io/v1`
//! `Lease` (chapter 27: "the operator elects through a Lease"; two replicas,
//! one acting).
//!
//! ```text
//! every pass: read Lease sprigdb-operator
//!   missing                                    -> create it, holding it
//!   held by me                                 -> renew (PUT with resourceVersion)
//!   held by another, renewTime + duration past -> take it over (PUT, transitions + 1)
//!   held by another and fresh                  -> follow
//! a conflict on the PUT means someone else wrote first: follow this pass
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sprig_common::time::Clock;

use crate::kube::{KubeApi, KubeError, KubeResult, LEASE};

/// `MicroTime`: RFC 3339 with six fractional digits.
#[must_use]
pub fn micro_time(t: SystemTime) -> String {
    let base = sprig_cluster::record::rfc3339(t);
    let micros = t
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_micros());
    format!("{}.{micros:06}Z", base.trim_end_matches('Z'))
}

/// Unix seconds of an RFC 3339 UTC time (`Z`), fractions ignored.
#[must_use]
#[allow(clippy::many_single_char_names)] // the civil-date formula's names
pub fn parse_utc(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (hh, mm, ss) = (n(11..13)?, n(14..16)?, n(17..19)?);
    // Howard Hinnant's days-from-civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hh * 3600 + mm * 60 + ss)
}

/// One candidate.
pub struct LeaseElector {
    kube: Arc<dyn KubeApi>,
    ns: String,
    name: String,
    identity: String,
    duration: Duration,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for LeaseElector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseElector")
            .field("ns", &self.ns)
            .field("name", &self.name)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl LeaseElector {
    /// `identity` competing for Lease `name` in `ns`, held `duration`.
    #[must_use]
    pub fn new(
        kube: Arc<dyn KubeApi>,
        ns: &str,
        name: &str,
        identity: &str,
        duration: Duration,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            kube,
            ns: ns.to_owned(),
            name: name.to_owned(),
            identity: identity.to_owned(),
            duration,
            clock,
        }
    }

    fn spec(&self, transitions: u64, acquire: Option<&str>) -> Value {
        let now = micro_time(self.clock.wall());
        let mut s = json!({
            "holderIdentity": self.identity,
            "leaseDurationSeconds": self.duration.as_secs(),
            "renewTime": now,
            "leaseTransitions": transitions,
        });
        s["acquireTime"] = json!(acquire.map_or(now, str::to_owned));
        s
    }

    /// Takes or renews the Lease; whether this candidate leads now.
    ///
    /// # Errors
    /// API failures other than a lost race.
    pub async fn try_lead(&self) -> KubeResult<bool> {
        let Some(mut cur) = self.kube.get(&LEASE, &self.ns, &self.name).await? else {
            let obj = json!({
                "apiVersion": LEASE.api_version(),
                "kind": LEASE.kind,
                "metadata": {"name": self.name, "namespace": self.ns},
                "spec": self.spec(0, None),
            });
            return match self.kube.create(&LEASE, &self.ns, &obj).await {
                Ok(_) => Ok(true),
                Err(KubeError::AlreadyExists) => Ok(false),
                Err(e) => Err(e),
            };
        };
        let spec = &cur["spec"];
        let holder = spec["holderIdentity"].as_str().unwrap_or_default();
        let transitions = spec["leaseTransitions"].as_u64().unwrap_or(0);
        let mine = holder == self.identity;
        if !mine && !holder.is_empty() {
            let renewed = spec["renewTime"].as_str().and_then(parse_utc).unwrap_or(0);
            let dur = spec["leaseDurationSeconds"]
                .as_i64()
                .unwrap_or(i64::try_from(self.duration.as_secs()).unwrap_or(15));
            let now = self
                .clock
                .wall()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
            if now < renewed + dur {
                return Ok(false);
            }
        }
        let acquire = spec["acquireTime"].as_str().map(str::to_owned);
        cur["spec"] = if mine {
            self.spec(transitions, acquire.as_deref())
        } else {
            self.spec(transitions + 1, None)
        };
        match self.kube.replace(&LEASE, &self.ns, &self.name, &cur).await {
            Ok(_) => Ok(true),
            Err(KubeError::Conflict(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        let t = UNIX_EPOCH + Duration::from_micros(1_762_646_400_123_456);
        let s = micro_time(t);
        assert_eq!(s, "2025-11-09T00:00:00.123456Z");
        assert_eq!(parse_utc(&s), Some(1_762_646_400));
        assert_eq!(parse_utc("2026-02-28T23:59:59Z"), Some(1_772_323_199));
        assert_eq!(parse_utc("nonsense"), None);
    }
}
