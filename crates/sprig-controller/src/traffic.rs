//! Traffic windows on the home DC's leader (chapter 24, "Aggregating
//! traffic statistics", "The migration decision"; build step 3).
//!
//! Gateways report reads and writes per (tenant, client DC) every
//! `routing.traffic_report_seconds` (`POST /admin/v1/traffic/reports`,
//! `sprig-gateway::dc`). The leader adds them to the tenant's ring of
//! one-minute buckets over `migration.window_minutes`; the ring is soft
//! state, refilled by new reports after a leader change.
//! `GET /admin/v1/tenants/{name}/traffic` (`sprigctl tenant traffic`)
//! shows the window and what R0's `recommend` mode would decide:
//!
//! ```text
//! w[d] = reads[d] + migration.write_weight * writes[d]   (known client DCs)
//! W = sum w[d]; no signal while W < migration.min_signal_requests
//! L(c) = sum over d of w[d] * rtt(d, c) / W
//! ```
//!
//! **R2.** With chapter 23's latency matrix ([`crate::latency`]) covering
//! every pair, `L(c)` is in milliseconds ([`TrafficView::weighted_rtt_ms`])
//! and the decision follows it.
//!
//! **Where the book differs (R0).** Without a latency matrix (before R2,
//! chapter 23 step 12, or while a pair is unmeasured), `rtt(d, c)` is 0
//! inside a DC and 1 between DCs:
//! `L(c)` is the share of weighted traffic that would cross DCs if `c`
//! were home. The candidate is the DC with the lowest `L`, ties to the
//! home; it qualifies when it beats the home by
//! `migration.min_improvement_pct`. Nothing moves on its own in R0: the
//! decision is shown, and `sprigctl tenant move` acts on it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{DcId, TenantId};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

const DC_GLOBAL: Scopes = Scopes::PROCESS;
const TENANT_DC_GLOBAL: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 24,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key! {
    /// `migration.window_minutes`.
    WINDOW_MINUTES: u64 = "migration.window_minutes", 30, DC_GLOBAL, 1, 1440,
    "Traffic window of the migration decision"
}
key! {
    /// `migration.write_weight`.
    WRITE_WEIGHT: u64 = "migration.write_weight", 3, DC_GLOBAL, 1, 100,
    "A write counts as this many reads"
}
key! {
    /// `migration.min_signal_requests`.
    MIN_SIGNAL_REQUESTS: u64 = "migration.min_signal_requests", 1000, DC_GLOBAL, 0, 1_000_000_000,
    "Weighted requests with DC ids needed before traffic counts"
}
key! {
    /// `migration.min_improvement_pct`.
    MIN_IMPROVEMENT_PCT: u64 = "migration.min_improvement_pct", 30, TENANT_DC_GLOBAL, 0, 100,
    "Relative gain a move needs"
}

/// One report row, as gateways send it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficRow {
    /// The tenant.
    pub tenant_id: u64,
    /// The client DC; `""` none, `"?"` unknown.
    pub client_dc: String,
    /// Reads.
    pub reads: u64,
    /// Writes.
    pub writes: u64,
}

/// One report (chapter 24's `TrafficBatch`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficBatch {
    /// Unix minute.
    pub minute: i64,
    /// Rows.
    pub rows: Vec<TrafficRow>,
}

/// Reads and writes.
type Counts = (u64, u64);

/// One tenant's ring: (minute, counts per client DC), oldest first.
type Ring = VecDeque<(i64, HashMap<String, Counts>)>;

/// `rtt(client_dc, candidate)` in ms, `None` when unmeasured.
pub type RttFn<'a> = dyn Fn(&str, &str) -> Option<f64> + 'a;

/// Every tenant's window on this leader.
#[derive(Debug, Default)]
pub struct TrafficWindows {
    rings: Mutex<HashMap<TenantId, Ring>>,
}

/// One client DC's line of a tenant's window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTraffic {
    /// The client DC; `""` none, `"?"` unknown.
    pub client_dc: String,
    /// Reads in the window.
    pub reads: u64,
    /// Writes in the window.
    pub writes: u64,
    /// `reads + write_weight * writes`.
    pub weighted: u64,
}

/// A tenant's window and the decision it would give.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrafficView {
    /// The tenant.
    pub tenant: String,
    /// Its home DC.
    pub home_dc: DcId,
    /// `migration.window_minutes`.
    pub window_minutes: u64,
    /// Minutes with reports in the window.
    pub minutes_reported: u64,
    /// Per client DC, largest weight first.
    pub clients: Vec<ClientTraffic>,
    /// `W`: weighted requests with known DC ids.
    pub weighted_total: u64,
    /// `W >= migration.min_signal_requests`.
    pub signal: bool,
    /// `L(c)` per candidate DC (R0: the share of weighted traffic that
    /// would cross DCs), in percent.
    pub cross_dc_pct: BTreeMap<String, f64>,
    /// `L(c)` per candidate DC in milliseconds, from the measured latency
    /// matrix (R2, chapter 23 step 12), when it covers every pair; the
    /// decision then uses it instead of [`TrafficView::cross_dc_pct`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub weighted_rtt_ms: BTreeMap<String, f64>,
    /// The best DC, if it qualifies as a move.
    pub candidate: Option<DcId>,
    /// `stay`, `candidate` or `no_signal`.
    pub decision: String,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl TrafficWindows {
    /// Adds a report, dropping minutes older than the window.
    pub fn add(&self, batch: &TrafficBatch, window_minutes: u64) {
        let oldest = batch.minute - i64::try_from(window_minutes).unwrap_or(i64::MAX);
        let mut rings = lock(&self.rings);
        for r in &batch.rows {
            let ring = rings.entry(TenantId(r.tenant_id)).or_default();
            let pos = ring.iter().position(|(m, _)| *m == batch.minute);
            let at = pos.unwrap_or_else(|| {
                // Reports arrive roughly in order; keep the ring sorted.
                let at = ring
                    .iter()
                    .position(|(m, _)| *m > batch.minute)
                    .unwrap_or(ring.len());
                ring.insert(at, (batch.minute, HashMap::new()));
                at
            });
            let bucket = &mut ring[at].1;
            let c = bucket.entry(r.client_dc.clone()).or_insert((0, 0));
            c.0 += r.reads;
            c.1 += r.writes;
            while ring.front().is_some_and(|(m, _)| *m <= oldest) {
                ring.pop_front();
            }
        }
        #[allow(clippy::cast_precision_loss)] // a gauge
        metrics::gauge!("sprig_controller_traffic_tenants").set(rings.len() as f64);
    }

    /// The window of `tenant` ending at `now_minute`, and the decision,
    /// with R0's 0/1 stand-in for `rtt` (see the module docs).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn view(
        &self,
        tenant: TenantId,
        name: &str,
        home: &DcId,
        now_minute: i64,
        window_minutes: u64,
        write_weight: u64,
        min_signal: u64,
        min_improvement_pct: u64,
    ) -> TrafficView {
        self.view_with_rtt(
            tenant,
            name,
            home,
            now_minute,
            window_minutes,
            write_weight,
            min_signal,
            min_improvement_pct,
            None,
        )
    }

    /// As [`Self::view`], weighing traffic by `rtt(client_dc, candidate)`
    /// in ms when given and it knows every pair needed (R2's measured
    /// matrix, `crate::latency::Matrix::rtt`); otherwise the 0/1 stand-in.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        clippy::cast_precision_loss,
        clippy::too_many_lines
    )]
    pub fn view_with_rtt(
        &self,
        tenant: TenantId,
        name: &str,
        home: &DcId,
        now_minute: i64,
        window_minutes: u64,
        write_weight: u64,
        min_signal: u64,
        min_improvement_pct: u64,
        rtt: Option<&RttFn<'_>>,
    ) -> TrafficView {
        let oldest = now_minute - i64::try_from(window_minutes).unwrap_or(i64::MAX);
        let mut sums: HashMap<String, Counts> = HashMap::new();
        let mut minutes = 0;
        if let Some(ring) = lock(&self.rings).get(&tenant) {
            for (m, b) in ring.iter().filter(|(m, _)| *m > oldest) {
                let _ = m;
                minutes += 1;
                for (dc, c) in b {
                    let s = sums.entry(dc.clone()).or_insert((0, 0));
                    s.0 += c.0;
                    s.1 += c.1;
                }
            }
        }
        let mut clients: Vec<ClientTraffic> = sums
            .into_iter()
            .map(|(client_dc, (reads, writes))| ClientTraffic {
                weighted: reads + write_weight * writes,
                client_dc,
                reads,
                writes,
            })
            .collect();
        clients.sort_by(|a, b| {
            b.weighted
                .cmp(&a.weighted)
                .then(a.client_dc.cmp(&b.client_dc))
        });
        let known: Vec<&ClientTraffic> = clients
            .iter()
            .filter(|c| DcId::new(c.client_dc.as_str()).is_ok())
            .collect();
        let total: u64 = known.iter().map(|c| c.weighted).sum();
        let mut cross = BTreeMap::new();
        let l = |c: &str| -> f64 {
            if total == 0 {
                return 0.0;
            }
            let local = known
                .iter()
                .find(|k| k.client_dc == c)
                .map_or(0, |k| k.weighted);
            (total - local) as f64 * 100.0 / total as f64
        };
        let home_l = l(home.as_str());
        cross.insert(home.to_string(), home_l);
        let mut best: Option<(String, f64)> = None;
        for k in &known {
            let v = l(&k.client_dc);
            cross.insert(k.client_dc.clone(), v);
            if k.client_dc != home.as_str() && best.as_ref().is_none_or(|(_, b)| v < *b) {
                best = Some((k.client_dc.clone(), v));
            }
        }
        // R2: L(c) = sum w[d] * rtt(d, c) / W in ms, when the matrix knows
        // every pair; the candidate and the decision then follow it.
        let mut measured = BTreeMap::new();
        if let Some(rtt) = rtt
            && total > 0
        {
            let cands: Vec<String> = std::iter::once(home.to_string())
                .chain(known.iter().map(|k| k.client_dc.clone()))
                .collect();
            let mut all = true;
            for c in &cands {
                let mut sum = 0.0;
                for k in &known {
                    match rtt(&k.client_dc, c) {
                        Some(ms) => sum += k.weighted as f64 * ms,
                        None => all = false,
                    }
                }
                measured.insert(c.clone(), sum / total as f64);
            }
            if all {
                let home_l = measured.get(home.as_str()).copied().unwrap_or(0.0);
                best = measured
                    .iter()
                    .filter(|(c, _)| c.as_str() != home.as_str())
                    .min_by(|a, b| a.1.total_cmp(b.1))
                    .map(|(c, v)| (c.clone(), *v));
                let signal = total >= min_signal && total > 0;
                let candidate = best.and_then(|(dc, v)| {
                    let gain = home_l - v;
                    (signal && gain > 0.0 && gain >= home_l * min_improvement_pct as f64 / 100.0)
                        .then(|| DcId::new(dc).ok())
                        .flatten()
                });
                let decision = if !signal {
                    "no_signal"
                } else if candidate.is_some() {
                    "candidate"
                } else {
                    "stay"
                };
                return TrafficView {
                    tenant: name.to_owned(),
                    home_dc: home.clone(),
                    window_minutes,
                    minutes_reported: minutes,
                    clients,
                    weighted_total: total,
                    signal,
                    cross_dc_pct: cross,
                    weighted_rtt_ms: measured,
                    candidate,
                    decision: decision.to_owned(),
                };
            }
            measured.clear();
        }
        let signal = total >= min_signal && total > 0;
        let candidate = best.and_then(|(dc, v)| {
            let gain = home_l - v;
            (signal && gain > 0.0 && gain >= home_l * min_improvement_pct as f64 / 100.0)
                .then(|| DcId::new(dc).ok())
                .flatten()
        });
        let decision = if !signal {
            "no_signal"
        } else if candidate.is_some() {
            "candidate"
        } else {
            "stay"
        };
        TrafficView {
            tenant: name.to_owned(),
            home_dc: home.clone(),
            window_minutes,
            minutes_reported: minutes,
            clients,
            weighted_total: total,
            signal,
            cross_dc_pct: cross,
            weighted_rtt_ms: measured,
            candidate,
            decision: decision.to_owned(),
        }
    }
}

/// The current Unix minute.
#[must_use]
pub fn unix_minute() -> i64 {
    let s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    i64::try_from(s / 60).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(t: u64, dc: &str, reads: u64, writes: u64) -> TrafficRow {
        TrafficRow {
            tenant_id: t,
            client_dc: dc.to_owned(),
            reads,
            writes,
        }
    }

    #[test]
    fn windows_weigh_writes_and_pick_the_busiest_dc() {
        let w = TrafficWindows::default();
        let fra1 = DcId::new("fra1").unwrap();
        w.add(
            &TrafficBatch {
                minute: 100,
                rows: vec![row(1, "fra1", 500, 100), row(1, "sin1", 4000, 2000)],
            },
            30,
        );
        w.add(
            &TrafficBatch {
                minute: 101,
                rows: vec![
                    row(1, "sin1", 500, 0),
                    row(1, "", 9000, 0),
                    row(1, "?", 5, 5),
                ],
            },
            30,
        );
        let v = w.view(TenantId(1), "a", &fra1, 101, 30, 3, 1000, 30);
        assert_eq!(v.minutes_reported, 2);
        // fra1 800, sin1 10500; the unknown and missing don't vote.
        assert_eq!(v.weighted_total, 11_300);
        assert_eq!(v.decision, "candidate");
        assert_eq!(v.candidate.unwrap().as_str(), "sin1");
        assert_eq!(v.clients[0].client_dc, "sin1");
        // Out of the window: nothing.
        let v = w.view(TenantId(1), "a", &fra1, 200, 30, 3, 1000, 30);
        assert_eq!((v.weighted_total, v.decision.as_str()), (0, "no_signal"));
        // Old minutes fall off the ring as new ones arrive.
        w.add(
            &TrafficBatch {
                minute: 140,
                rows: vec![row(1, "fra1", 2000, 0)],
            },
            30,
        );
        let v = w.view(TenantId(1), "a", &fra1, 140, 30, 3, 1000, 30);
        assert_eq!(v.decision, "stay");
        assert_eq!(v.minutes_reported, 1);
    }

    #[test]
    fn measured_latency_weighs_the_traffic() {
        // Clients in ams1 and mum1; home fra1. By share alone mum1 wins
        // (most traffic); measured RTTs say ams1 is close to everyone.
        let w = TrafficWindows::default();
        let fra1 = DcId::new("fra1").unwrap();
        w.add(
            &TrafficBatch {
                minute: 100,
                rows: vec![row(1, "mum1", 6000, 0), row(1, "ams1", 4000, 0)],
            },
            30,
        );
        let share = w.view(TenantId(1), "a", &fra1, 100, 30, 3, 1000, 10);
        assert_eq!(share.candidate.as_ref().unwrap().as_str(), "mum1");
        assert!(share.weighted_rtt_ms.is_empty());
        let rtt = |a: &str, b: &str| -> Option<f64> {
            let mut p = [a, b];
            p.sort_unstable();
            match p {
                [x, y] if x == y => Some(0.2),
                ["ams1", "fra1"] => Some(10.0),
                ["ams1", "mum1"] => Some(110.0),
                ["fra1", "mum1"] => Some(120.0),
                _ => None,
            }
        };
        let v = w.view_with_rtt(TenantId(1), "a", &fra1, 100, 30, 3, 1000, 10, Some(&rtt));
        // L(fra1) = 0.6*120 + 0.4*10 = 76; L(ams1) = 0.6*110 + 0.4*0.2 = 66.08;
        // L(mum1) = 0.6*0.2 + 0.4*110 = 44.12.
        assert!((v.weighted_rtt_ms["fra1"] - 76.0).abs() < 1e-6);
        assert!((v.weighted_rtt_ms["mum1"] - 44.12).abs() < 1e-6);
        assert_eq!(v.candidate.unwrap().as_str(), "mum1");
        // An unmeasured pair: back to the share.
        let partial = |a: &str, b: &str| (a == b).then_some(0.2);
        let v = w.view_with_rtt(
            TenantId(1),
            "a",
            &fra1,
            100,
            30,
            3,
            1000,
            10,
            Some(&partial),
        );
        assert!(v.weighted_rtt_ms.is_empty());
        assert_eq!(v.candidate, share.candidate);
    }
}
