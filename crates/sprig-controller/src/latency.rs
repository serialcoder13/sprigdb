//! The latency matrix and the DC lifecycle (chapter 23, "Measuring the
//! latency matrix" and "Bootstrapping the first DC and adding later
//! ones"; build steps 12 and 13).
//!
//! ```text
//! latency loop, each DC leader, every control.latency_probe_seconds, for each
//! other DC d in state joining, active or suspect:
//! 1. Peer.Ping a controller of d (from dc-observe/<d>); record RTT or failure
//! 2. over a 5-minute window compute p50, p90, p99
//! 3. write latency/<self>/<d> if p50 moved by > control.latency_publish_change_pct,
//!    reachability changed, or 10 minutes passed
//! rtt(d, d) = 0.2 ms (the book's floor; gateways don't report writer RTTs yet)
//!
//! dc_registry loop, global leader, every 5 s:
//! - joining -> active once dc-leases/<dc> exists and latency rows exist both ways
//!   between the DC and every other active DC
//! - draining -> retired once no tenant is homed in the DC
//! ```
//!
//! Readers ([`Matrix`]) ignore rows older than 30 minutes. Chapter 24's
//! move recommendation weighs traffic by these RTTs ([`crate::traffic`]).
//!
//! **Where it differs from the book.** `Peer.Ping` is
//! `GET /internal/v1/peer/ping` on the controllers' internal address
//! (decision F2), found in `dc-observe/<d>`; the book's long-lived mTLS
//! channel is a pooled HTTP/1.1 connection.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::{DcRecord, rfc3339};
use sprig_meta::dclease::{dc_key, lease_key};
use sprig_meta::permit::http::JsonHttp;
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Tier, Versioned};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::failover::observe::{ObserveTargets, PeerPing, targets_key};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// `control.latency_probe_seconds`.
pub static LATENCY_PROBE_SECONDS: Setting<u64> = Setting {
    key: "control.latency_probe_seconds",
    default: 5,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 3600),
    apply: Apply::Live,
    operator_only: true,
    owner: 23,
    doc: "Ping period between data centers",
};
sprig_settings::register!(LATENCY_PROBE_SECONDS);

/// `control.latency_publish_change_pct`.
pub static LATENCY_PUBLISH_CHANGE_PCT: Setting<u64> = Setting {
    key: "control.latency_publish_change_pct",
    default: 10,
    scopes: Scopes(0b10_0000),
    hard: HardBounds::range(0, 1000),
    apply: Apply::Live,
    operator_only: true,
    owner: 23,
    doc: "Change in p50 that republishes a latency row",
};
sprig_settings::register!(LATENCY_PUBLISH_CHANGE_PCT);

/// The sample window.
pub const WINDOW: Duration = Duration::from_secs(300);
/// A row is republished at least this often.
pub const REPUBLISH: Duration = Duration::from_secs(600);
/// Readers ignore rows older than this.
pub const STALE_MS: u64 = 30 * 60 * 1000;
/// `rtt(d, d)`.
pub const LOCAL_RTT_MS: f64 = 0.2;

/// `latency/<from>/<to>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyRow {
    /// Median RTT in the window, ms.
    pub p50_ms: f64,
    /// 90th percentile, ms.
    pub p90_ms: f64,
    /// 99th percentile, ms.
    pub p99_ms: f64,
    /// Whether the last probe got an answer.
    pub reachable: bool,
    /// Samples in the window.
    pub samples: u32,
    /// When written, Unix ms (for readers' staleness).
    pub at_ms: u64,
    /// When written, for people.
    pub at: String,
}

impl Versioned for LatencyRow {
    const V: u32 = 1;
}

/// The key `latency/<from>/<to>`.
///
/// # Errors
/// Internal never in practice.
pub fn row_key(from: &DcId, to: &DcId) -> Result<sprig_meta::Key> {
    Tier::Global.key(&format!("latency/{from}/{to}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The `q` quantile (nearest rank) of sorted `v`.
#[must_use]
pub fn quantile(v: &[f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let rank = ((q * v.len() as f64).ceil() as usize).clamp(1, v.len());
    v[rank - 1]
}

/// One destination's window.
#[derive(Debug, Default)]
struct Window {
    samples: VecDeque<(Instant, Option<f64>)>,
    published: Option<(Instant, f64, bool)>,
}

impl Window {
    fn add(&mut self, now: Instant, rtt: Option<f64>) {
        self.samples.push_back((now, rtt));
        while self
            .samples
            .front()
            .is_some_and(|(t, _)| now.saturating_duration_since(*t) > WINDOW)
        {
            self.samples.pop_front();
        }
    }

    /// The row to publish now, if one is due.
    fn due(&mut self, now: Instant, change_pct: u64) -> Option<LatencyRow> {
        let mut ok: Vec<f64> = self.samples.iter().filter_map(|(_, r)| *r).collect();
        ok.sort_by(f64::total_cmp);
        let reachable = self.samples.back().is_some_and(|(_, r)| r.is_some());
        let p50 = quantile(&ok, 0.5);
        let publish = match self.published {
            None => true,
            Some((at, last, was)) => {
                #[allow(clippy::cast_precision_loss)]
                let moved = last <= 0.0 || ((p50 - last).abs() * 100.0 / last) > change_pct as f64;
                was != reachable || moved || now.saturating_duration_since(at) >= REPUBLISH
            }
        };
        if !publish {
            return None;
        }
        self.published = Some((now, p50, reachable));
        Some(LatencyRow {
            p50_ms: p50,
            p90_ms: quantile(&ok, 0.9),
            p99_ms: quantile(&ok, 0.99),
            reachable,
            samples: u32::try_from(ok.len()).unwrap_or(u32::MAX),
            at_ms: now_ms(),
            at: rfc3339(SystemTime::now()),
        })
    }
}

/// The DC leader's `latency` loop and the global leader's `dc_registry`
/// loop (see the module docs).
pub struct LatencyProber {
    dc: DcId,
    global: Arc<dyn MetaStore>,
    live: Option<LiveSettings>,
    http: JsonHttp,
    windows: Mutex<HashMap<DcId, Window>>,
}

impl std::fmt::Debug for LatencyProber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyProber")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

async fn dcs(global: &dyn MetaStore) -> Result<Vec<(Record<DcRecord>, u64)>> {
    let page = global
        .list(&Tier::Global.key("dcs/")?, ListOpts::default())
        .await?;
    page.kvs
        .iter()
        .map(|kv| Ok((Record::<DcRecord>::decode(&kv.value)?, kv.mod_revision)))
        .collect()
}

impl LatencyProber {
    /// A prober for `dc`.
    #[must_use]
    pub fn new(dc: DcId, global: Arc<dyn MetaStore>, live: Option<LiveSettings>) -> Arc<Self> {
        Arc::new(Self {
            dc,
            global,
            live,
            http: JsonHttp::new(Duration::from_secs(2)),
            windows: Mutex::new(HashMap::new()),
        })
    }

    fn period(&self) -> Duration {
        let s = self
            .live
            .as_ref()
            .map_or(LATENCY_PROBE_SECONDS.default, |l| {
                l.get(&LATENCY_PROBE_SECONDS, &SettingCtx::process(&self.dc))
            });
        Duration::from_secs(s.max(1))
    }

    fn change_pct(&self) -> u64 {
        self.live
            .as_ref()
            .map_or(LATENCY_PUBLISH_CHANGE_PCT.default, |l| {
                l.get(&LATENCY_PUBLISH_CHANGE_PCT, &SettingCtx::process(&self.dc))
            })
    }

    /// One `Peer.Ping` round trip to a controller of `d`, ms.
    async fn ping(&self, d: &DcId) -> Option<f64> {
        let kv = self
            .global
            .get(&targets_key(d).ok()?, Consistency::Serializable)
            .await
            .ok()??;
        let t = Record::<ObserveTargets>::decode(&kv.value).ok()?.body;
        for c in &t.controllers {
            let url = format!("{}/internal/v1/peer/ping", c.trim_end_matches('/'));
            let t0 = Instant::now();
            if self
                .http
                .call::<(), PeerPing>(hyper::Method::GET, &url, None)
                .await
                .is_ok()
            {
                return Some(t0.elapsed().as_secs_f64() * 1000.0);
            }
        }
        None
    }

    /// One probing round: every other DC pinged, due rows written.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn probe_round(&self) -> Result<usize> {
        let mut written = 0;
        for (rec, _) in dcs(&*self.global).await? {
            let d = rec.body.dc_id.clone();
            if d == self.dc || !matches!(rec.body.state.as_str(), "joining" | "active" | "suspect")
            {
                continue;
            }
            // The first sample of a new pooled connection pays the TCP
            // handshake: ping twice and keep the second.
            let rtt = match self.ping(&d).await {
                Some(_) => self.ping(&d).await,
                None => None,
            };
            if let Some(ms) = rtt {
                metrics::gauge!("sprig_controller_dc_rtt_seconds", "to_dc" => d.to_string())
                    .set(ms / 1000.0);
            }
            let row = {
                let mut w = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
                let win = w.entry(d.clone()).or_default();
                let now = Instant::now();
                win.add(now, rtt);
                win.due(now, self.change_pct())
            };
            if let Some(row) = row {
                self.global
                    .txn(sprig_meta::Txn {
                        when: Vec::new(),
                        then: vec![Op::Put {
                            key: row_key(&self.dc, &d)?,
                            value: Record::new(row).encode()?,
                            lease: None,
                        }],
                        otherwise: Vec::new(),
                    })
                    .await?;
                written += 1;
            }
        }
        Ok(written)
    }

    /// One `dc_registry` pass on the global leader: `joining` DCs become
    /// `active`, `draining` ones `retired` (see the module docs). Returns
    /// the changes made.
    ///
    /// # Errors
    /// The store's errors; 39504 when leadership was lost.
    pub async fn registry_round(&self, cx: &LoopCx) -> Result<Vec<(DcId, String)>> {
        let l = cx.leader()?;
        let all = dcs(&*self.global).await?;
        let matrix = Matrix::read(&*self.global).await?;
        let mut changed = Vec::new();
        for (rec, rev) in &all {
            let d = &rec.body.dc_id;
            let next = match rec.body.state.as_str() {
                "joining" => {
                    let leased = self
                        .global
                        .get(&lease_key(d)?, Consistency::Linearizable)
                        .await?
                        .is_some();
                    let peers: Vec<&DcId> = all
                        .iter()
                        .filter(|(r, _)| r.body.dc_id != *d && r.body.state == "active")
                        .map(|(r, _)| &r.body.dc_id)
                        .collect();
                    let measured = peers
                        .iter()
                        .all(|p| matrix.row(d, p).is_some() && matrix.row(p, d).is_some());
                    (leased && measured).then_some("active")
                }
                "draining" => {
                    let homed = homed_in(&*self.global, d).await?;
                    (homed == 0).then_some("retired")
                }
                _ => None,
            };
            let Some(next) = next else {
                continue;
            };
            let mut r = rec.clone();
            next.clone_into(&mut r.body.state);
            let key = dc_key(d)?;
            let ok = l
                .commit_if(
                    vec![Cmp::ModRev(key.clone(), *rev)],
                    vec![Op::Put {
                        key,
                        value: r.encode()?,
                        lease: None,
                    }],
                )
                .await?
                .succeeded;
            if ok {
                tracing::warn!(dc = %d, from = %rec.body.state, to = next, "DC state changed (dc_registry)");
                changed.push((d.clone(), next.to_owned()));
            }
        }
        Ok(changed)
    }
}

/// Tenants whose directory entry says `home_dc = d`.
async fn homed_in(global: &dyn MetaStore, d: &DcId) -> Result<usize> {
    let page = global
        .list(&Tier::Global.key("tenants/")?, ListOpts::default())
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter(|kv| {
            serde_json::from_slice::<serde_json::Value>(&kv.value)
                .ok()
                .is_some_and(|v| v["home_dc"] == d.as_str() && v["state"] != "deleting")
        })
        .count())
}

/// The measured matrix, as readers see it: fresh rows only.
#[derive(Debug, Clone, Default)]
pub struct Matrix {
    rows: BTreeMap<(DcId, DcId), LatencyRow>,
}

impl Matrix {
    /// Reads `latency/` (rows older than 30 minutes are dropped).
    ///
    /// # Errors
    /// The store's errors.
    pub async fn read(global: &dyn MetaStore) -> Result<Self> {
        let page = global
            .list(&Tier::Global.key("latency/")?, ListOpts::default())
            .await?;
        let now = now_ms();
        let mut rows = BTreeMap::new();
        for kv in page.kvs {
            let path = kv.key.as_str().trim_start_matches("/sprig/g/latency/");
            let Some((a, b)) = path.split_once('/') else {
                continue;
            };
            let (Ok(a), Ok(b)) = (DcId::new(a), DcId::new(b)) else {
                continue;
            };
            let Ok(r) = Record::<LatencyRow>::decode(&kv.value) else {
                continue;
            };
            if now.saturating_sub(r.body.at_ms) > STALE_MS {
                continue;
            }
            rows.insert((a, b), r.body);
        }
        Ok(Self { rows })
    }

    /// The row `from -> to`, if fresh.
    #[must_use]
    pub fn row(&self, from: &DcId, to: &DcId) -> Option<&LatencyRow> {
        self.rows.get(&(from.clone(), to.clone()))
    }

    /// `rtt(from, to)` in ms: [`LOCAL_RTT_MS`] inside a DC; the measured
    /// p50 (the larger of the two directions' rows when both exist);
    /// `None` when unmeasured or unreachable.
    #[must_use]
    pub fn rtt(&self, from: &str, to: &str) -> Option<f64> {
        if from == to {
            return Some(LOCAL_RTT_MS);
        }
        let (a, b) = (DcId::new(from).ok()?, DcId::new(to).ok()?);
        let one = |x: &DcId, y: &DcId| {
            self.row(x, y)
                .filter(|r| r.reachable && r.samples > 0)
                .map(|r| r.p50_ms.max(LOCAL_RTT_MS))
        };
        match (one(&a, &b), one(&b, &a)) {
            (Some(x), Some(y)) => Some(x.max(y)),
            (Some(x), None) | (None, Some(x)) => Some(x),
            (None, None) => None,
        }
    }

    /// Whether no row is fresh.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Every fresh row.
    #[must_use]
    pub fn rows(&self) -> &BTreeMap<(DcId, DcId), LatencyRow> {
        &self.rows
    }
}

/// Which of the two loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Probe,
    Registry,
}

struct Unit {
    p: Arc<LatencyProber>,
    kind: Kind,
}

#[async_trait::async_trait]
impl Reconciler for Unit {
    type Key = &'static str;

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Probe => "latency",
            Kind::Registry => "dc_registry",
        }
    }

    fn tier(&self) -> LoopTier {
        match self.kind {
            Kind::Probe => LoopTier::DcLeader,
            Kind::Registry => LoopTier::GlobalLeader,
        }
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<Self::Key> {
        vec!["dcs"]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<Self::Key>> {
        Ok(vec!["dcs"])
    }

    async fn reconcile(&self, cx: &LoopCx, _k: &Self::Key) -> Result<Outcome> {
        match self.kind {
            Kind::Probe => {
                cx.leader()?;
                self.p.probe_round().await?;
                Ok(Outcome::RequeueAfter(self.p.period()))
            }
            Kind::Registry => {
                self.p.registry_round(cx).await?;
                Ok(Outcome::RequeueAfter(Duration::from_secs(5)))
            }
        }
    }
}

impl LatencyProber {
    /// The `latency` (DC leader) and `dc_registry` (global leader) loops.
    #[must_use]
    pub fn loops(self: &Arc<Self>) -> Vec<crate::runtime::Loop> {
        vec![
            crate::runtime::Loop::new(Unit {
                p: Arc::clone(self),
                kind: Kind::Probe,
            }),
            crate::runtime::Loop::new(Unit {
                p: Arc::clone(self),
                kind: Kind::Registry,
            }),
        ]
    }
}

/// `sprigctl dc drain` and `sprigctl dc undrain` on `dcs/<dc>`: `active`
/// to `draining` (no new tenants; migration empties it; `dc_registry`
/// retires it once empty), and back.
///
/// # Errors
/// 39503 for an unknown DC; 39505-like refusals as invalid requests for a
/// DC in another state; the store's errors.
pub async fn set_draining(global: &dyn MetaStore, dc: &DcId, drain: bool) -> Result<DcRecord> {
    let key = dc_key(dc)?;
    let (from, to) = if drain {
        ("active", "draining")
    } else {
        ("draining", "active")
    };
    loop {
        let kv = global
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(SprigCode::UnknownDc, format!("unknown data center {dc}"))
            })?;
        let mut rec = Record::<DcRecord>::decode(&kv.value)?;
        if rec.body.state == to {
            return Ok(rec.body);
        }
        if rec.body.state != from {
            return Err(SprigError::new(
                SprigCode::ConfigInvalid,
                format!(
                    "data center {dc} is {}; only an {from} one becomes {to}",
                    rec.body.state
                ),
            ));
        }
        to.clone_into(&mut rec.body.state);
        let r = global
            .txn(sprig_meta::Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: rec.encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            sprig_common::audit::record(
                sprig_common::audit::AuditStream::Platform,
                "admin.dc_state",
                serde_json::json!({"dc": dc.as_str(), "from": from, "to": to}),
            );
            return Ok(rec.body);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_use_the_nearest_rank() {
        let v = [1.0, 2.0, 3.0, 4.0, 10.0];
        assert!((quantile(&v, 0.5) - 3.0).abs() < 1e-9);
        assert!((quantile(&v, 0.9) - 10.0).abs() < 1e-9);
        assert!((quantile(&[], 0.5)).abs() < 1e-9);
    }

    #[test]
    fn rows_republish_on_change_reachability_or_age() {
        let mut w = Window::default();
        let t0 = Instant::now();
        w.add(t0, Some(30.0));
        assert!(w.due(t0, 10).is_some(), "the first row");
        w.add(t0 + Duration::from_secs(5), Some(31.0));
        assert!(w.due(t0 + Duration::from_secs(5), 10).is_none(), "3% moved");
        for i in 0..10 {
            w.add(t0 + Duration::from_secs(10 + i), Some(60.0));
        }
        assert!(
            w.due(t0 + Duration::from_secs(20), 10).is_some(),
            "p50 doubled"
        );
        w.add(t0 + Duration::from_secs(25), None);
        let r = w
            .due(t0 + Duration::from_secs(25), 10)
            .expect("unreachable now");
        assert!(!r.reachable);
    }

    #[test]
    fn the_matrix_reads_both_directions_and_the_local_floor() {
        let mut m = Matrix::default();
        let (a, b) = (DcId::new("fra1").unwrap(), DcId::new("ams1").unwrap());
        let row = |p50| LatencyRow {
            p50_ms: p50,
            p90_ms: p50,
            p99_ms: p50,
            reachable: true,
            samples: 3,
            at_ms: now_ms(),
            at: String::new(),
        };
        m.rows.insert((a.clone(), b.clone()), row(20.0));
        assert_eq!(m.rtt("fra1", "fra1"), Some(LOCAL_RTT_MS));
        assert_eq!(m.rtt("fra1", "ams1"), Some(20.0));
        assert_eq!(m.rtt("ams1", "fra1"), Some(20.0));
        m.rows.insert((b, a), row(24.0));
        assert_eq!(m.rtt("fra1", "ams1"), Some(24.0));
        assert_eq!(m.rtt("fra1", "wit1"), None);
    }
}
