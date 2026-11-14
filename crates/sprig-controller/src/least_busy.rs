//! Least-busy DC placement.
//!
//! ```text
//! dc-load/<dc> (global tier), written by that DC's leader after each
//! balancer run when a value moved 2 points, the DC's lease epoch or state
//! changed, or the record is 2 minutes old:
//!   storage_spare, cpu_spare, conn_spare = 1 - used/capacity over the DC's
//!     placeable clusters (the balancer's fleet view); cpu counts busy time
//!     and probe pressure; a dimension no cluster declares is 1 (unknown:
//!     no preference)
//!   clusters = placeable clusters; stable_since_ms = when the DC last
//!   became active under its current lease epoch
//! least_busy(A):          # A = allowed, active DCs
//!   drop DCs without a placeable cluster
//!   score(d) = ws*storage_spare + wc*cpu_spare + wn*conn_spare
//!              (placement.least_busy_weights, normalized; a record older
//!              than 5 minutes scores 0, fully busy)
//!   argmax, ties to the lower id
//! a new tenant without a --cluster hint: home = least_busy(stable A), where
//!   stable = active for recovery.stability_minutes; when no DC is stable
//!   yet (a new deployment) every active one counts; none -> 39525
//! ```
//!
//! The decision loop ([`crate::dc_migrate`]) uses the same scores for ties
//! and relief moves.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_balancer::model::{ClusterState, FleetView};
use sprig_balancer::settings::LeastBusyWeights;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// A record older than this scores as fully busy.
pub const STALE_MS: u64 = 5 * 60 * 1000;
/// A record is rewritten at least this often.
pub const REFRESH_MS: u64 = 2 * 60 * 1000;
/// A value moving this much is written at once.
pub const PUBLISH_STEP: f64 = 0.02;

/// `placement.dc_relief_spare_pct`.
pub static DC_RELIEF_SPARE_PCT: Setting<u64> = Setting {
    key: "placement.dc_relief_spare_pct",
    default: 15,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 100),
    apply: Apply::Live,
    operator_only: true,
    owner: 24,
    doc: "Spare share below which a home DC sheds tenants",
};
sprig_settings::register!(DC_RELIEF_SPARE_PCT);

/// `placement.least_busy_margin`.
pub static LEAST_BUSY_MARGIN: Setting<f64> = Setting {
    key: "placement.least_busy_margin",
    default: 0.15,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 1),
    apply: Apply::Live,
    operator_only: true,
    owner: 24,
    doc: "Score gain a relief move needs",
};
sprig_settings::register!(LEAST_BUSY_MARGIN);

/// `dc-load/<dc_id>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcLoad {
    /// The DC.
    pub dc_id: DcId,
    /// `1 - used/capacity` of storage over its placeable clusters.
    pub storage_spare: f64,
    /// The same for busy time (and probe pressure).
    pub cpu_spare: f64,
    /// The same for connections.
    pub conn_spare: f64,
    /// Placeable clusters.
    pub clusters: u32,
    /// The DC lease epoch the record was written under.
    pub dc_epoch: u64,
    /// When the DC became active under that epoch, Unix ms.
    pub stable_since_ms: u64,
    /// When written, Unix ms.
    pub at_ms: u64,
}

impl Versioned for DcLoad {
    const V: u32 = 1;
}

/// The key `dc-load/<dc>`.
///
/// # Errors
/// Internal never in practice.
pub fn load_key(dc: &DcId) -> Result<sprig_meta::Key> {
    Tier::Global.key(&format!("dc-load/{dc}"))
}

fn clamp01(v: f64) -> f64 {
    if v.is_finite() {
        v.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

/// Spare storage, CPU and connections of `dc`'s placeable clusters in
/// `view`, and how many there are.
#[must_use]
pub fn spare_of(view: &FleetView, dc: &DcId) -> (f64, f64, f64, u32) {
    let loads = view.loads();
    let (mut su, mut sc, mut bu, mut bc, mut cu, mut cc) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    let mut pressure: f64 = 0.0;
    let mut n = 0u32;
    for c in view.clusters.values() {
        if &c.dc != dc || c.state != ClusterState::Placeable {
            continue;
        }
        n += 1;
        let l = loads.get(&c.id).copied().unwrap_or_default();
        if c.cap.storage_bytes > 0.0 {
            su += l.storage_bytes;
            sc += c.cap.storage_bytes;
        }
        if c.cap.busy_ms_per_sec > 0.0 {
            bu += l.busy_ms_per_sec;
            bc += c.cap.busy_ms_per_sec;
        }
        if c.cap.connections > 0.0 {
            cu += l.connections;
            cc += c.cap.connections;
        }
        pressure = pressure.max(c.pressure.value(c.cap.threads));
    }
    let spare = |u: f64, c: f64| if c > 0.0 { clamp01(1.0 - u / c) } else { 1.0 };
    let cpu = spare(bu, bc).min(clamp01(1.0 - pressure));
    (spare(su, sc), cpu, spare(cu, cc), n)
}

/// `score(d)` (module docs): 0 for a missing or stale record.
#[must_use]
pub fn score(load: Option<&DcLoad>, w: &LeastBusyWeights, now_ms: u64) -> f64 {
    let Some(l) = load else {
        return 0.0;
    };
    if now_ms.saturating_sub(l.at_ms) > STALE_MS {
        return 0.0;
    }
    let sum = w.storage + w.cpu + w.connections;
    if sum <= 0.0 {
        return 0.0;
    }
    (w.storage * l.storage_spare + w.cpu * l.cpu_spare + w.connections * l.conn_spare) / sum
}

/// `least_busy(A)` (module docs): the DC of `allowed` with the highest
/// score among those with a placeable cluster (a missing or stale record
/// counts as none), ties to the lower id.
#[must_use]
pub fn least_busy(
    allowed: &[DcId],
    loads: &BTreeMap<DcId, DcLoad>,
    w: &LeastBusyWeights,
    now_ms: u64,
) -> Option<(DcId, f64)> {
    let mut best: Option<(DcId, f64)> = None;
    let mut sorted: Vec<&DcId> = allowed.iter().collect();
    sorted.sort();
    for d in sorted {
        let l = loads.get(d);
        if l.is_none_or(|l| l.clusters == 0 || now_ms.saturating_sub(l.at_ms) > STALE_MS) {
            continue;
        }
        let s = score(l, w, now_ms);
        if best.as_ref().is_none_or(|(_, b)| s > *b) {
            best = Some((d.clone(), s));
        }
    }
    best
}

/// Every `dc-load/` record.
///
/// # Errors
/// The store's errors.
pub async fn read_all(global: &dyn MetaStore) -> Result<BTreeMap<DcId, DcLoad>> {
    let page = global
        .list(&Tier::Global.key("dc-load/")?, ListOpts::default())
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<DcLoad>::decode(&kv.value).ok())
        .map(|r| (r.body.dc_id.clone(), r.body))
        .collect())
}

/// Whether `old` must be rewritten with `new` (module docs).
#[must_use]
pub fn due(old: Option<&DcLoad>, new: &DcLoad) -> bool {
    let Some(o) = old else {
        return true;
    };
    o.dc_epoch != new.dc_epoch
        || o.clusters != new.clusters
        || o.stable_since_ms != new.stable_since_ms
        || (o.storage_spare - new.storage_spare).abs() >= PUBLISH_STEP
        || (o.cpu_spare - new.cpu_spare).abs() >= PUBLISH_STEP
        || (o.conn_spare - new.conn_spare).abs() >= PUBLISH_STEP
        || new.at_ms.saturating_sub(o.at_ms) >= REFRESH_MS
}

/// Writes `dc`'s record from the balancer's `view` when due; returns it.
///
/// # Errors
/// The store's errors.
pub async fn publish(
    global: &Arc<dyn MetaStore>,
    dc: &DcId,
    view: &FleetView,
    now_ms: u64,
) -> Result<Option<DcLoad>> {
    let rec = crate::dc::list(&**global)
        .await?
        .into_iter()
        .find(|d| &d.dc_id == dc);
    let Some(rec) = rec else {
        return Ok(None);
    };
    let key = load_key(dc)?;
    let old = global
        .get(&key, Consistency::Serializable)
        .await?
        .and_then(|kv| Record::<DcLoad>::decode(&kv.value).ok())
        .map(|r| r.body);
    let (storage_spare, cpu_spare, conn_spare, clusters) = spare_of(view, dc);
    for (r, v) in [
        ("storage", storage_spare),
        ("cpu", cpu_spare),
        ("connections", conn_spare),
    ] {
        metrics::gauge!("sprig_controller_dc_spare_ratio", "dc" => dc.to_string(), "resource" => r)
            .set(v);
    }
    let active = rec.state == "active";
    let stable_since_ms = match &old {
        _ if !active => 0,
        Some(o) if o.dc_epoch == rec.dc_epoch && o.stable_since_ms > 0 => o.stable_since_ms,
        _ => now_ms,
    };
    let new = DcLoad {
        dc_id: dc.clone(),
        storage_spare,
        cpu_spare,
        conn_spare,
        clusters,
        dc_epoch: rec.dc_epoch,
        stable_since_ms,
        at_ms: now_ms,
    };
    if !due(old.as_ref(), &new) {
        return Ok(old);
    }
    global
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: Record::new(new.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(Some(new))
}

/// Where a new tenant goes when its creator gave no `--cluster` hint
/// (module docs): the DC and why (`least_busy`, `only_dc`).
///
/// # Errors
/// 39525 when no allowed, active DC has a placeable cluster.
pub async fn place_new_tenant(
    global: &dyn MetaStore,
    allowed: Option<&BTreeSet<DcId>>,
    w: &LeastBusyWeights,
    stability_ms: u64,
    now_ms: u64,
) -> Result<(DcId, &'static str, BTreeMap<DcId, f64>)> {
    let dcs = crate::dc::list(global).await?;
    let loads = read_all(global).await?;
    let active: Vec<DcId> = dcs
        .iter()
        .filter(|d| d.state == "active" && d.kind != "witness")
        .filter(|d| allowed.is_none_or(|a| a.contains(&d.dc_id)))
        .map(|d| d.dc_id.clone())
        .collect();
    let stable: Vec<DcId> = active
        .iter()
        .filter(|d| {
            loads.get(*d).is_some_and(|l| {
                l.stable_since_ms > 0 && now_ms.saturating_sub(l.stable_since_ms) >= stability_ms
            })
        })
        .cloned()
        .collect();
    let scores: BTreeMap<DcId, f64> = active
        .iter()
        .map(|d| (d.clone(), score(loads.get(d), w, now_ms)))
        .collect();
    let pick =
        least_busy(&stable, &loads, w, now_ms).or_else(|| least_busy(&active, &loads, w, now_ms));
    match pick {
        Some((d, _)) => {
            let reason = if active.len() == 1 {
                "only_dc"
            } else {
                "least_busy"
            };
            metrics::counter!("sprig_controller_least_busy_placements_total", "reason" => "new")
                .increment(1);
            Ok((d, reason, scores))
        }
        None => Err(SprigError::new(
            SprigCode::NoDcCapacity,
            format!(
                "no data center can take a new tenant: allowed and active {}, none with a fresh \
                 dc-load record and a placeable cluster",
                if active.is_empty() {
                    "none".to_owned()
                } else {
                    active
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::many_single_char_names, clippy::float_cmp, clippy::unwrap_used)]

    use super::*;
    use sprig_balancer::model::{Capacity, ClusterView, LoadVector, ShardKey, ShardView};
    use sprig_common::ids::ClusterId;

    fn d(s: &str) -> DcId {
        DcId::new(s).unwrap()
    }

    fn rec(dc: &str, s: f64, c: f64, n: f64, at: u64) -> DcLoad {
        DcLoad {
            dc_id: d(dc),
            storage_spare: s,
            cpu_spare: c,
            conn_spare: n,
            clusters: 1,
            dc_epoch: 1,
            stable_since_ms: 1,
            at_ms: at,
        }
    }

    #[test]
    fn the_books_worked_example() {
        // fra1 (0.30, 0.50, 0.60) scores 0.44, ams1 (0.55, 0.40, 0.70) 0.52.
        let w = LeastBusyWeights::default();
        let now = 1_000_000;
        let loads: BTreeMap<DcId, DcLoad> = [
            (d("fra1"), rec("fra1", 0.30, 0.50, 0.60, now)),
            (d("ams1"), rec("ams1", 0.55, 0.40, 0.70, now)),
        ]
        .into_iter()
        .collect();
        assert!((score(loads.get(&d("fra1")), &w, now) - 0.44).abs() < 1e-9);
        assert!((score(loads.get(&d("ams1")), &w, now) - 0.52).abs() < 1e-9);
        let (best, _) = least_busy(&[d("fra1"), d("ams1")], &loads, &w, now).unwrap();
        assert_eq!(best, d("ams1"));
        // Residency first: only fra1 allowed.
        assert_eq!(
            least_busy(&[d("fra1")], &loads, &w, now).unwrap().0,
            d("fra1")
        );
        // A stale record scores as fully busy and is skipped.
        let later = now + STALE_MS + 1;
        let mut l2 = loads.clone();
        l2.get_mut(&d("fra1")).unwrap().at_ms = later;
        assert_eq!(
            least_busy(&[d("fra1"), d("ams1")], &l2, &w, later)
                .unwrap()
                .0,
            d("fra1")
        );
        assert_eq!(score(l2.get(&d("ams1")), &w, later), 0.0);
        // Ties go to the lower id; a DC without a placeable cluster is out.
        let mut l3 = loads.clone();
        l3.insert(d("aaa1"), rec("aaa1", 0.55, 0.40, 0.70, now));
        assert_eq!(
            least_busy(&[d("ams1"), d("aaa1")], &l3, &w, now).unwrap().0,
            d("aaa1")
        );
        l3.get_mut(&d("aaa1")).unwrap().clusters = 0;
        assert_eq!(
            least_busy(&[d("ams1"), d("aaa1")], &l3, &w, now).unwrap().0,
            d("ams1")
        );
    }

    #[test]
    fn spare_from_the_fleet_view() {
        let gb = 1024.0 * 1024.0 * 1024.0;
        let mut v = FleetView::default();
        let c = ClusterId::new("fra1-c1").unwrap();
        v.add_cluster(ClusterView::new(
            c.clone(),
            d("fra1"),
            Capacity {
                storage_bytes: 10.0 * gb,
                connections: 100.0,
                ..Capacity::default()
            },
        ));
        v.add_shard(ShardView::new(
            ShardKey::new(5, 0, 0),
            c,
            LoadVector {
                storage_bytes: 4.0 * gb,
                connections: 25.0,
                ..LoadVector::default()
            },
        ));
        let (s, cpu, n, k) = spare_of(&v, &d("fra1"));
        assert!((s - 0.6).abs() < 1e-9);
        assert!((n - 0.75).abs() < 1e-9);
        // Busy time isn't declared: unknown, no preference.
        assert!((cpu - 1.0).abs() < 1e-9);
        assert_eq!(k, 1);
        assert_eq!(spare_of(&v, &d("ams1")).3, 0);
    }

    /// A recovered DC looks idle: until it has been active for
    /// `recovery.stability_minutes`, new tenants go elsewhere (no wave of
    /// tenants into a DC still catching up).
    #[tokio::test]
    async fn a_recovered_dc_gets_no_wave_of_new_tenants() {
        use sprig_meta::bootstrap::DcRecord;
        let global = sprig_meta::MemStore::new(Tier::Global);
        let now = 100 * 60_000;
        let put = |key: String, value: Vec<u8>| Op::Put {
            key: Tier::Global.key(&key).unwrap(),
            value,
            lease: None,
        };
        let mut ops = Vec::new();
        for (dc, spare, since) in [("ams1", 0.9, now - 60_000), ("fra1", 0.5, 1)] {
            ops.push(put(
                format!("dcs/{dc}"),
                Record::new(DcRecord {
                    dc_id: d(dc),
                    kind: "full".into(),
                    state: "active".into(),
                    dc_epoch: 1,
                    tags: serde_json::Map::new(),
                    global_voter: false,
                })
                .encode()
                .unwrap(),
            ));
            let mut l = rec(dc, spare, spare, spare, now);
            l.stable_since_ms = since;
            ops.push(put(
                format!("dc-load/{dc}"),
                Record::new(l).encode().unwrap(),
            ));
        }
        global
            .txn(Txn {
                when: Vec::new(),
                then: ops,
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
        let w = LeastBusyWeights::default();
        // ams1 came back a minute ago: fra1, though busier, gets the tenant.
        let (dc, _, _) = place_new_tenant(&global, None, &w, 60 * 60_000, now)
            .await
            .unwrap();
        assert_eq!(dc, d("fra1"));
        // Once ams1 is stable (or the setting is 0), it wins.
        let (dc, _, _) = place_new_tenant(&global, None, &w, 0, now).await.unwrap();
        assert_eq!(dc, d("ams1"));
        // Residency first.
        let only: BTreeSet<DcId> = [d("ams1")].into_iter().collect();
        let (dc, _, _) = place_new_tenant(&global, Some(&only), &w, 60 * 60_000, now)
            .await
            .unwrap();
        assert_eq!(
            dc,
            d("ams1"),
            "no stable allowed DC: every active allowed one counts"
        );
        // Nothing allowed: 39525.
        let none: BTreeSet<DcId> = [d("sin1")].into_iter().collect();
        let e = place_new_tenant(&global, Some(&none), &w, 0, now)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::NoDcCapacity));
    }

    #[test]
    fn publishing_is_damped() {
        let a = rec("fra1", 0.5, 0.5, 0.5, 1000);
        let mut b = a.clone();
        b.at_ms = 61_000;
        b.storage_spare = 0.51;
        assert!(!due(Some(&a), &b));
        b.storage_spare = 0.52;
        assert!(due(Some(&a), &b));
        let mut c = a.clone();
        c.at_ms = 1000 + REFRESH_MS;
        assert!(due(Some(&a), &c));
        let mut e = a.clone();
        e.dc_epoch = 2;
        assert!(due(Some(&a), &e));
        assert!(due(None, &a));
    }
}
