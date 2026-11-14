//! The pure parts of the global failover decision: steps 3, 5 and 7 of the
//! outline in [`super::global`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sprig_common::ids::DcId;

use super::record::{StandbyLag, Verdict, VerdictKind};

/// Step 3: whether automatic DC failover is possible at all.
///
/// `voter_sites` is how many sites the global voters span; `available` how
/// many active sites other than the lost DC hold valid DC leases.
///
/// # Errors
/// The reason the failover is `manual_only`.
pub fn sites_check(voter_sites: usize, available: usize, min_sites: u32) -> Result<(), String> {
    let min = min_sites as usize;
    if voter_sites < min {
        return Err(format!(
            "the global voters span {voter_sites} sites, fewer than failover.min_sites {min}"
        ));
    }
    if available + 1 < min {
        return Err(format!(
            "only {available} other sites hold DC leases; failover.min_sites {min} needs {}",
            min - 1
        ));
    }
    Ok(())
}

/// Step 5's tally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tally {
    /// Enough `down` verdicts and no `alive`.
    FailOver,
    /// Not yet: ask again (until the decision timeout).
    Retry(String),
}

/// Step 5: at least `min_sites - 1` `down` (or `unsafe`, whose writers are
/// then skipped) verdicts from observers holding valid leases, and no
/// `alive`.
#[must_use]
pub fn tally(verdicts: &[Verdict], min_sites: u32) -> Tally {
    let valid: Vec<&Verdict> = verdicts.iter().filter(|v| v.observer_lease_valid).collect();
    if let Some(a) = valid.iter().find(|v| v.verdict == VerdictKind::Alive) {
        return Tally::Retry(format!(
            "{} sees the DC alive (a split view: the DC fenced itself)",
            a.observer_dc
        ));
    }
    let down = valid
        .iter()
        .filter(|v| matches!(v.verdict, VerdictKind::Down | VerdictKind::Unsafe))
        .count();
    let need = min_sites.saturating_sub(1) as usize;
    if down >= need {
        Tally::FailOver
    } else {
        Tally::Retry(format!("{down} of {need} sites agree the DC is down"))
    }
}

/// Writers some observer found unsafe.
#[must_use]
pub fn unsafe_writers(verdicts: &[Verdict]) -> BTreeSet<String> {
    verdicts
        .iter()
        .filter(|v| v.observer_lease_valid)
        .flat_map(|v| v.unsafe_writers.iter().cloned())
        .collect()
}

/// Standby lags reported per (standby DC, tenant).
#[must_use]
pub fn lags(verdicts: &[Verdict]) -> BTreeMap<(DcId, u64), u32> {
    let mut out = BTreeMap::new();
    for v in verdicts.iter().filter(|v| v.observer_lease_valid) {
        for StandbyLag { tenant_id, lag_ms } in &v.lags {
            out.insert((v.observer_dc.clone(), *tenant_id), *lag_ms);
        }
    }
    out
}

/// What step 7 needs about one tenant homed in the lost DC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantCandidate {
    /// The tenant.
    pub tenant_id: u64,
    /// Its directory epoch.
    pub epoch: u64,
    /// `failover.mode = manual` for the tenant.
    pub manual: bool,
    /// `failover.rpo_seconds` for the tenant.
    pub rpo: Duration,
    /// Standby DCs that hold every unit, best first.
    pub standby_dcs: Vec<DcId>,
    /// Writers (`cluster/addr`) of the clusters holding the tenant in the
    /// lost DC.
    pub writers: Vec<String>,
    /// Pinned to the lost DC (`sprigctl tenant pin`): never moved.
    pub pinned: bool,
    /// The tenant's allowed DCs (residency); `None`: unrestricted.
    pub allowed_dcs: Option<BTreeSet<DcId>>,
}

/// Step 7 for one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantPlan {
    /// Promote the standby in `to`.
    Promote {
        /// The standby DC.
        to: DcId,
        /// Its lag.
        lag_ms: u32,
    },
    /// Stay fenced in the lost DC.
    Skip(String),
}

/// Step 7: skip a manual or pinned tenant, one without a standby holding
/// every unit, one whose standby's lag is unknown or over its RPO, or one
/// on an unsafe writer; else promote the best standby that qualifies and
/// that the tenant's residency allows.
#[must_use]
pub fn plan_tenant(
    t: &TenantCandidate,
    lags: &BTreeMap<(DcId, u64), u32>,
    unsafe_writers: &BTreeSet<String>,
) -> TenantPlan {
    if t.manual {
        return TenantPlan::Skip("failover.mode is manual".into());
    }
    if t.pinned {
        return TenantPlan::Skip("pinned to the lost DC".into());
    }
    if let Some(w) = t.writers.iter().find(|w| unsafe_writers.contains(*w)) {
        return TenantPlan::Skip(format!("writer {w} is reachable and can't self-fence"));
    }
    if t.standby_dcs.is_empty() {
        return TenantPlan::Skip("no standby DC holds every unit".into());
    }
    let mut why = Vec::new();
    for dc in &t.standby_dcs {
        // Residency: a standby outside the allowed DCs is
        // never promoted, whatever its lag.
        if let Some(a) = &t.allowed_dcs
            && !a.contains(dc)
        {
            why.push(format!("{dc}: outside the tenant's residency"));
            continue;
        }
        match lags.get(&(dc.clone(), t.tenant_id)) {
            None => why.push(format!("{dc}: lag unknown")),
            Some(&lag) if u128::from(lag) > t.rpo.as_millis() => why.push(format!(
                "{dc}: lag {:.1} s > rpo {} s",
                f64::from(lag) / 1000.0,
                t.rpo.as_secs()
            )),
            Some(&lag) => {
                return TenantPlan::Promote {
                    to: dc.clone(),
                    lag_ms: lag,
                };
            }
        }
    }
    TenantPlan::Skip(why.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dc(s: &str) -> DcId {
        DcId::new(s).expect("dc")
    }

    fn v(obs: &str, kind: VerdictKind, valid: bool) -> Verdict {
        Verdict {
            observer_dc: dc(obs),
            observer_lease_valid: valid,
            verdict: kind,
            unsafe_writers: vec![],
            lags: vec![],
        }
    }

    #[test]
    fn too_few_sites_means_manual_only() {
        assert!(sites_check(3, 2, 3).is_ok());
        assert!(sites_check(2, 2, 3).is_err(), "voters in two sites");
        assert!(sites_check(3, 1, 3).is_err(), "one other site left");
    }

    #[test]
    fn two_down_verdicts_and_no_alive_fail_over() {
        let d = [
            v("ams1", VerdictKind::Down, true),
            v("wit1", VerdictKind::Down, true),
        ];
        assert_eq!(tally(&d, 3), Tally::FailOver);
        let one = [
            v("ams1", VerdictKind::Down, true),
            v("wit1", VerdictKind::Down, false),
        ];
        assert!(
            matches!(tally(&one, 3), Tally::Retry(_)),
            "a fenced observer doesn't count"
        );
        let split = [
            v("ams1", VerdictKind::Down, true),
            v("wit1", VerdictKind::Down, true),
            v("iad1", VerdictKind::Alive, true),
        ];
        assert!(matches!(tally(&split, 3), Tally::Retry(w) if w.contains("alive")));
    }

    fn cand() -> TenantCandidate {
        TenantCandidate {
            tenant_id: 42,
            epoch: 3,
            manual: false,
            rpo: Duration::from_secs(5),
            standby_dcs: vec![dc("ams1"), dc("iad1")],
            writers: vec!["fra1-c1/10.0.0.1:3306".into()],
            pinned: false,
            allowed_dcs: None,
        }
    }

    #[test]
    fn a_standby_outside_the_residency_is_never_promoted() {
        let mut l = BTreeMap::new();
        l.insert((dc("ams1"), 42), 9200);
        l.insert((dc("iad1"), 42), 800);
        // iad1 is the better standby but outside the EU.
        let t = TenantCandidate {
            allowed_dcs: Some([dc("fra1"), dc("ams1")].into()),
            rpo: Duration::from_secs(10),
            ..cand()
        };
        assert_eq!(
            plan_tenant(&t, &l, &BTreeSet::new()),
            TenantPlan::Promote {
                to: dc("ams1"),
                lag_ms: 9200
            }
        );
        // Only iad1 within the RPO: skipped, never iad1.
        let t = TenantCandidate {
            allowed_dcs: Some([dc("fra1"), dc("ams1")].into()),
            ..cand()
        };
        assert!(
            matches!(plan_tenant(&t, &l, &BTreeSet::new()), TenantPlan::Skip(w) if w.contains("residency"))
        );
        // Fail closed: an empty set promotes nothing.
        let t = TenantCandidate {
            allowed_dcs: Some(BTreeSet::new()),
            ..cand()
        };
        assert!(matches!(
            plan_tenant(&t, &l, &BTreeSet::new()),
            TenantPlan::Skip(_)
        ));
    }

    #[test]
    fn tenants_promote_to_the_first_standby_within_the_rpo() {
        let mut l = BTreeMap::new();
        l.insert((dc("ams1"), 42), 9200);
        l.insert((dc("iad1"), 42), 800);
        assert_eq!(
            plan_tenant(&cand(), &l, &BTreeSet::new()),
            TenantPlan::Promote {
                to: dc("iad1"),
                lag_ms: 800
            }
        );
        l.insert((dc("iad1"), 42), 9000);
        assert!(
            matches!(plan_tenant(&cand(), &l, &BTreeSet::new()), TenantPlan::Skip(w) if w.contains("rpo"))
        );
        assert!(
            matches!(plan_tenant(&cand(), &BTreeMap::new(), &BTreeSet::new()), TenantPlan::Skip(w) if w.contains("unknown"))
        );
    }

    #[test]
    fn manual_pinned_unsafe_and_standbyless_tenants_are_skipped() {
        let mut l = BTreeMap::new();
        l.insert((dc("ams1"), 42), 100);
        let u: BTreeSet<String> = ["fra1-c1/10.0.0.1:3306".to_owned()].into();
        assert!(
            matches!(plan_tenant(&cand(), &l, &u), TenantPlan::Skip(w) if w.contains("self-fence"))
        );
        let t = TenantCandidate {
            manual: true,
            ..cand()
        };
        assert!(matches!(
            plan_tenant(&t, &l, &BTreeSet::new()),
            TenantPlan::Skip(_)
        ));
        let t = TenantCandidate {
            pinned: true,
            ..cand()
        };
        assert!(matches!(
            plan_tenant(&t, &l, &BTreeSet::new()),
            TenantPlan::Skip(_)
        ));
        let t = TenantCandidate {
            standby_dcs: vec![],
            ..cand()
        };
        assert!(matches!(
            plan_tenant(&t, &l, &BTreeSet::new()),
            TenantPlan::Skip(_)
        ));
    }
}
