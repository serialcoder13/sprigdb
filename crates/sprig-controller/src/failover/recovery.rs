//! Recovery after an outage (chapter 25, "Recovery after an outage"): the
//! returning DC stays fenced, diverged transactions are reported, traffic
//! returns in steps.
//!
//! ```text
//!  fenced ──► checking ──┬─► catching_up ──┐
//!                        │                 ├──► warming ──► stable
//!                        └─► rebuilding ───┘   (traffic in steps)
//! ```
//!
//! - [`ReturningDc`]: the returning DC's leader. While `dcs/X.state` is
//!   `returning` (set by the global leader when X holds a lease again),
//!   every tenant X still holds a home map for but the directory shows
//!   moved (another home, or a higher epoch) is fenced through
//!   [`CopyFence`] and its maps marked `quarantined`; only then does X set
//!   itself `active`, which is when its controllers grant permits again.
//! - [`errant_report`]: the diverged-transaction report (count, GTIDs,
//!   tables, statements) for the failover record. Nothing is replayed.
//! - [`TrafficSteps`] and [`Admission`]: traffic returns in steps; a
//!   returning DC's gateways admit new connections at a rate raised on the
//!   same schedule, and refuse the rest with 39531
//!   `ER_SPRIG_ADMISSION_BACKOFF` and a jittered retry delay of 1-5 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::failover::primary_replica::{DivergedRange, DivergedTxn};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::dclease::dc_key;
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn};
use sprig_shard::home::read_home;

use super::promote::quarantine_maps;
use super::record::ErrantReport;

/// Fences one tenant's copy in the returning DC (revoke write grants,
/// `KILL USER`, or the mover's rename fence; chapter 10's fences).
#[async_trait::async_trait]
pub trait CopyFence: Send + Sync {
    /// Fences `tenant`'s copy in this DC; idempotent.
    async fn fence(&self, tenant: TenantId) -> Result<()>;
}

/// What one [`ReturningDc::step`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnStep {
    /// The DC isn't returning.
    NotReturning,
    /// Moved tenants were fenced and quarantined; the DC is `active`.
    Unfenced {
        /// Tenants fenced and quarantined here.
        fenced: Vec<TenantId>,
    },
}

/// The returning DC's leader side.
pub struct ReturningDc {
    /// This DC.
    pub dc: DcId,
    /// The global tier.
    pub global: Arc<dyn MetaStore>,
    /// This DC's tier.
    pub tier: Arc<dyn MetaStore>,
    /// The copy fence.
    pub fence: Arc<dyn CopyFence>,
}

impl std::fmt::Debug for ReturningDc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReturningDc").field("dc", &self.dc).finish_non_exhaustive()
    }
}

impl ReturningDc {
    /// One pass (see the module docs).
    ///
    /// # Errors
    /// The store's and the fence's errors; the DC then stays `returning`
    /// (fenced) and the next pass tries again.
    pub async fn step(&self) -> Result<ReturnStep> {
        let dkey = dc_key(&self.dc)?;
        let Some(kv) = self.global.get(&dkey, Consistency::Linearizable).await? else {
            return Ok(ReturnStep::NotReturning);
        };
        let mut rec = Record::<DcRecord>::decode(&kv.value)?;
        if rec.body.state != "returning" {
            return Ok(ReturnStep::NotReturning);
        }
        let fenced = self.fence_moved().await?;
        "active".clone_into(&mut rec.body.state);
        let r = self
            .global
            .txn(Txn {
                when: vec![Cmp::ModRev(dkey.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: dkey,
                    value: rec.encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            return Err(SprigError::new(
                SprigCode::CatalogConflict,
                "the DC record changed while returning; trying again",
            ));
        }
        tracing::info!(dc = %self.dc, fenced = fenced.len(), "returned: moved tenants fenced; DC active");
        Ok(ReturnStep::Unfenced { fenced })
    }

    /// Fences and quarantines every tenant this DC holds a home map for
    /// that the directory shows elsewhere (or at a higher epoch).
    async fn fence_moved(&self) -> Result<Vec<TenantId>> {
        let prefix = self.tier.tier().key("shardmaps/")?;
        let page = self.tier.list(&prefix, ListOpts::default()).await?;
        let mut tenants: Vec<(TenantId, u64)> = Vec::new();
        for kv in page.kvs {
            let Ok(m) = Record::<ShardMap>::decode(&kv.value) else {
                continue;
            };
            if m.body.role != MapRole::Home {
                continue;
            }
            let t = m.body.tenant_id;
            if !tenants.iter().any(|(x, _)| *x == t) {
                tenants.push((t, m.body.tenant_epoch));
            }
        }
        let mut fenced = Vec::new();
        for (t, map_epoch) in tenants {
            let Some(e) = read_home(&*self.global, t).await? else {
                continue;
            };
            if e.home_dc == self.dc && e.epoch <= map_epoch {
                continue;
            }
            self.fence.fence(t).await?;
            quarantine_maps(&*self.tier, t).await?;
            tracing::warn!(dc = %self.dc, tenant = %t, home = %e.home_dc, "moved tenant fenced and quarantined");
            fenced.push(t);
        }
        Ok(fenced)
    }
}

/// The diverged-transaction report from a returning copy's range and the
/// transactions read from its binlog, keeping only those that touch
/// `schemas` (the moved tenant's; empty keeps all): a cluster shared by
/// several tenants logs the others' writes in the same GTID domain.
#[must_use]
pub fn errant_report(range: &DivergedRange, txns: &[DivergedTxn], schemas: &[&str]) -> ErrantReport {
    let ours = |t: &&DivergedTxn| {
        schemas.is_empty()
            || t.tables.iter().any(|tb| {
                schemas
                    .iter()
                    .any(|s| tb.split_once('.').is_some_and(|(db, _)| db == *s))
            })
    };
    let kept: Vec<&DivergedTxn> = txns.iter().filter(ours).collect();
    let mut tables: Vec<String> = kept.iter().flat_map(|t| t.tables.iter().cloned()).collect();
    tables.sort();
    tables.dedup();
    ErrantReport {
        count: if schemas.is_empty() {
            range.count()
        } else {
            kept.len() as u64
        },
        gtids: if schemas.is_empty() {
            vec![format!(
                "{}-{}-{}..{}",
                range.domain,
                range.server,
                range.after_seq + 1,
                range.through_seq
            )]
        } else {
            kept.iter().map(|t| t.gtid.clone()).collect()
        },
        tables,
        path: None,
        statements: kept.iter().flat_map(|t| t.statements.iter().cloned()).collect(),
    }
}

/// Traffic returning in steps (`recovery.traffic_steps_pct`, one step per
/// `recovery.step_minutes`, advancing while lag and errors stay low; a
/// breach steps back once).
#[derive(Debug, Clone)]
pub struct TrafficSteps {
    steps: Vec<u32>,
    step: Duration,
    at: usize,
    since: Instant,
    stepped_back: bool,
}

impl TrafficSteps {
    /// Starts at the first step at `now`.
    #[must_use]
    pub fn new(steps: Vec<u32>, step: Duration, now: Instant) -> Self {
        Self {
            steps: if steps.is_empty() { vec![100] } else { steps },
            step,
            at: 0,
            since: now,
            stepped_back: false,
        }
    }

    /// The current share, percent.
    #[must_use]
    pub fn pct(&self) -> u32 {
        self.steps[self.at]
    }

    /// Whether the last step (100 %) is reached.
    #[must_use]
    pub fn done(&self) -> bool {
        self.at + 1 == self.steps.len()
    }

    /// The step index.
    #[must_use]
    pub fn index(&self) -> usize {
        self.at
    }

    /// Advances one step per period while `healthy`; an unhealthy check
    /// steps back once (and restarts the period). Returns the share.
    pub fn tick(&mut self, now: Instant, healthy: bool) -> u32 {
        if !healthy {
            if !self.stepped_back && self.at > 0 {
                self.at -= 1;
                self.stepped_back = true;
            }
            self.since = now;
            return self.pct();
        }
        if !self.done() && now.saturating_duration_since(self.since) >= self.step {
            self.at += 1;
            self.since = now;
            self.stepped_back = false;
        }
        self.pct()
    }

    /// Connections each gateway admits per second at the current step:
    /// `base` at the first step, raised in proportion, unlimited (0) once
    /// done.
    #[must_use]
    pub fn admit_per_second(&self, base: u32) -> u32 {
        if self.done() {
            return 0;
        }
        let first = self.steps[0].max(1);
        let rate = u64::from(base) * u64::from(self.pct()) / u64::from(first);
        u32::try_from(rate).unwrap_or(u32::MAX).max(1)
    }
}

/// A gateway's admission of new connections while its DC returns: a token
/// bucket refilled at the permit's `admit_per_second` (0: no limit).
#[derive(Debug, Clone)]
pub struct Admission {
    tokens: f64,
    last: Instant,
    seed: u64,
}

impl Admission {
    /// An empty bucket at `now` (a returning gateway starts with none).
    #[must_use]
    pub fn new(now: Instant, seed: u64) -> Self {
        Self {
            tokens: 0.0,
            last: now,
            seed: seed | 1,
        }
    }

    fn jitter(&mut self) -> Duration {
        // xorshift64: full jitter between 1 and 5 s, no shared RNG needed.
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        Duration::from_millis(1000 + self.seed % 4001)
    }

    /// Admits one connection or returns 39531 with a retry delay.
    ///
    /// # Errors
    /// 39531 `ER_SPRIG_ADMISSION_BACKOFF` (SQLSTATE 08S01).
    pub fn admit(&mut self, now: Instant, rate: u32) -> Result<()> {
        if rate == 0 {
            return Ok(());
        }
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        let cap = f64::from(rate);
        self.tokens = (self.tokens + dt * cap).min(cap);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        let wait = self.jitter();
        Err(SprigError::new(
            SprigCode::AdmissionBackoff,
            format!(
                "this data center is returning from an outage and admits connections in steps; \
                 retry in {} ms",
                wait.as_millis()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_keeps_the_moved_tenants_schemas() {
        let range = DivergedRange { domain: 1, server: 101, after_seq: 10, through_seq: 13 };
        let t = |g: &str, tb: &str| DivergedTxn { gtid: g.into(), tables: vec![tb.into()], statements: vec![format!("INSERT {tb}")] };
        let txns = [t("1-101-11", "t1.la"), t("1-101-12", "t2.la"), t("1-101-13", "t1.other")];
        let all = errant_report(&range, &txns, &[]);
        assert_eq!(all.count, 3);
        let mine = errant_report(&range, &txns, &["t1"]);
        assert_eq!(mine.count, 2);
        assert_eq!(mine.gtids, vec!["1-101-11".to_owned(), "1-101-13".to_owned()]);
        assert_eq!(mine.tables, vec!["t1.la".to_owned(), "t1.other".to_owned()]);
    }

    #[test]
    fn steps_advance_and_step_back_once() {
        let t0 = Instant::now();
        let m = Duration::from_secs(300);
        let mut s = TrafficSteps::new(vec![10, 25, 50, 100], m, t0);
        assert_eq!(s.pct(), 10);
        assert_eq!(s.tick(t0 + Duration::from_secs(100), true), 10);
        assert_eq!(s.tick(t0 + m, true), 25);
        assert_eq!(s.tick(t0 + m * 2, true), 50);
        assert_eq!(s.tick(t0 + m * 2 + Duration::from_secs(10), false), 25, "a breach steps back");
        assert_eq!(s.tick(t0 + m * 2 + Duration::from_secs(20), false), 25, "only once");
        assert_eq!(s.tick(t0 + m * 3 + Duration::from_secs(20), true), 50);
        assert_eq!(s.tick(t0 + m * 4 + Duration::from_secs(20), true), 100);
        assert!(s.done());
        assert_eq!(s.admit_per_second(200), 0, "unlimited at 100 %");
    }

    #[test]
    fn admission_rises_with_the_steps() {
        let t0 = Instant::now();
        let mut s = TrafficSteps::new(vec![10, 25, 50, 100], Duration::from_secs(1), t0);
        assert_eq!(s.admit_per_second(200), 200);
        s.tick(t0 + Duration::from_secs(1), true);
        assert_eq!(s.admit_per_second(200), 500);
    }

    #[test]
    fn a_reconnect_storm_is_admitted_at_the_rate() {
        // 10,000 clients reconnecting at once to one gateway at 200/s: about
        // 200 per second get in, the rest get 39531 with 1-5 s hints.
        let t0 = Instant::now();
        let mut a = Admission::new(t0, 7);
        let mut admitted = 0;
        let mut hints = Vec::new();
        for i in 0..10_000u64 {
            // All within the first second.
            let now = t0 + Duration::from_micros(i * 100);
            match a.admit(now, 200) {
                Ok(()) => admitted += 1,
                Err(e) => {
                    assert_eq!(e.code(), Some(SprigCode::AdmissionBackoff));
                    hints.push(e.message);
                }
            }
        }
        assert!((190..=200).contains(&admitted), "admitted {admitted}");
        let ms: Vec<u64> = hints
            .iter()
            .filter_map(|m| m.rsplit("retry in ").next()?.trim_end_matches(" ms").parse().ok())
            .collect();
        assert_eq!(ms.len(), hints.len());
        assert!(ms.iter().all(|m| (1000..=5000).contains(m)));
        let spread = ms.iter().max().unwrap_or(&0) - ms.iter().min().unwrap_or(&0);
        assert!(spread > 3500, "retries spread over 1-5 s: {spread}");
        assert!(a.admit(t0, 0).is_ok(), "rate 0 admits everyone");
    }
}
