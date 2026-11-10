//! Items in the DC tier (`/sprig/dc/<dc>/approvals/<n>`) and the global
//! tier (`/sprig/g/approvals/<n>`): cluster and DC failovers held for
//! approval, which must stay readable when a tenant's home DC is lost
//! (chapter 52, "Where recommendations live").
//!
//! The tiers hold few items (failovers only), so a raise lists them all
//! and writes with compare-and-swaps on every key it read or wrote, plus
//! the counter `approvals-seq`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Record, Tier, Txn, Versioned};

use crate::machine::{self, Verdict, VerdictPlan};
use crate::model::{Micros, Proposal, RecId, Recommendation, State};
use crate::policy::Policy;
use crate::store::{DUE_APPLYING_TIMEOUT, Filter, Raised, RecStore};

impl Versioned for Recommendation {
    const V: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Seq {
    next: u64,
}

impl Versioned for Seq {
    const V: u32 = 1;
}

/// A tier's items.
pub struct EtcdRecStore {
    store: Arc<dyn MetaStore>,
    tier: Tier,
}

impl std::fmt::Debug for EtcdRecStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdRecStore")
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

impl EtcdRecStore {
    /// The items of `tier` in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, tier: Tier) -> Self {
        Self { store, tier }
    }

    fn id(&self, n: u64) -> RecId {
        match &self.tier {
            Tier::Global => RecId::Global(n),
            Tier::Dc(dc) => RecId::Dc(dc.clone(), n),
        }
    }

    fn key(&self, n: u64) -> Result<Key> {
        self.tier.key(&format!("approvals/{n:020}"))
    }

    fn seq_key(&self) -> Result<Key> {
        self.tier.key("approvals-seq")
    }

    /// Every item with its mod revision.
    async fn all(&self) -> Result<Vec<(Recommendation, u64)>> {
        let prefix = self.tier.key("approvals/")?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<Recommendation>::decode(&kv.value) {
                    Ok(r) => out.push((r.body, kv.mod_revision)),
                    Err(e) => tracing::warn!(key = %kv.key, error = %e, "unreadable approval item"),
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    fn cmp_rev(key: Key, rev: u64) -> Cmp {
        if rev == 0 {
            Cmp::Missing(key)
        } else {
            Cmp::ModRev(key, rev)
        }
    }
}

#[async_trait::async_trait]
impl RecStore for EtcdRecStore {
    async fn raise(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        for _ in 0..5 {
            let all = self.all().await?;
            let fp = p.fingerprint();
            let same: Vec<Recommendation> = all
                .iter()
                .filter(|(r, _)| r.kind == p.kind && r.target == p.target && r.state.is_live())
                .map(|(r, _)| r.clone())
                .collect();
            let ignored = all
                .iter()
                .filter(|(r, _)| r.fingerprint == fp && r.state == State::Ignored)
                .max_by_key(|(r, _)| r.updated_at_us)
                .map(|(r, _)| r.clone());
            let seq_key = self.seq_key()?;
            let seq = self.store.get(&seq_key, Consistency::Linearizable).await?;
            let (next, seq_rev) = match &seq {
                Some(kv) => (Record::<Seq>::decode(&kv.value)?.body.next, kv.mod_revision),
                None => (1, 0),
            };
            let plan = machine::raise(
                p,
                policy,
                &same,
                ignored.as_ref(),
                ancestry,
                self.id(next),
                now,
            );
            let mut when = Vec::new();
            let mut then = Vec::new();
            let mut stored = Vec::new();
            let mut used_seq = false;
            for w in &plan.writes {
                let n = w.id.number();
                let rev = if w.id == self.id(next) && !all.iter().any(|(x, _)| x.id == w.id) {
                    used_seq = true;
                    0
                } else {
                    all.iter()
                        .find(|(x, _)| x.id == w.id)
                        .map_or(0, |(_, rev)| *rev)
                };
                when.push(Self::cmp_rev(self.key(n)?, rev));
                then.push(Op::Put {
                    key: self.key(n)?,
                    value: Record::new(w.clone()).encode()?,
                    lease: None,
                });
                stored.push(w.clone());
            }
            if used_seq {
                when.push(Self::cmp_rev(seq_key.clone(), seq_rev));
                then.push(Op::Put {
                    key: seq_key,
                    value: Record::new(Seq { next: next + 1 }).encode()?,
                    lease: None,
                });
            }
            if !then.is_empty() {
                let r = self
                    .store
                    .txn(Txn {
                        when,
                        then,
                        otherwise: Vec::new(),
                    })
                    .await?;
                if !r.succeeded {
                    continue;
                }
            }
            let mut out = Raised::from_plan(&plan, &stored);
            if plan.verdict == VerdictPlan::Suppressed
                && let Some(i) = ignored
            {
                out.verdict = Verdict::Suppressed(i.id);
            }
            return Ok(out);
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            "approval items kept changing during the raise",
        ))
    }

    async fn get(&self, n: u64) -> Result<Option<Recommendation>> {
        let kv = self
            .store
            .get(&self.key(n)?, Consistency::Linearizable)
            .await?;
        kv.map(|kv| Record::<Recommendation>::decode(&kv.value).map(|r| r.body))
            .transpose()
    }

    async fn list(&self, f: &Filter) -> Result<Vec<Recommendation>> {
        let mut v: Vec<Recommendation> = self
            .all()
            .await?
            .into_iter()
            .map(|(r, _)| r)
            .filter(|r| f.matches(r))
            .collect();
        v.sort_by(|a, b| b.id.number().cmp(&a.id.number()));
        v.truncate(f.limit());
        Ok(v)
    }

    async fn put(&self, r: &Recommendation, expected: u64) -> Result<bool> {
        let key = self.key(r.id.number())?;
        let Some(kv) = self.store.get(&key, Consistency::Linearizable).await? else {
            return Ok(false);
        };
        let cur = Record::<Recommendation>::decode(&kv.value)?.body;
        if cur.version != expected {
            return Ok(false);
        }
        let res = self
            .store
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key,
                    value: Record::new(r.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(res.succeeded)
    }

    async fn due(&self, now: Micros, limit: usize) -> Result<Vec<Recommendation>> {
        Ok(self
            .all()
            .await?
            .into_iter()
            .map(|(r, _)| r)
            .filter(|r| machine::next_due(r, DUE_APPLYING_TIMEOUT).is_some_and(|t| t <= now))
            .take(limit)
            .collect())
    }

    async fn purge(&self, before: Micros) -> Result<u64> {
        let mut n = 0;
        for (r, rev) in self.all().await? {
            let keep_ignore =
                r.state == State::Ignored && r.ignore.is_some_and(|i| i.until_us.is_none());
            if r.state.is_closed() && r.updated_at_us < before && !keep_ignore {
                let key = self.key(r.id.number())?;
                let res = self
                    .store
                    .txn(Txn {
                        when: vec![Cmp::ModRev(key.clone(), rev)],
                        then: vec![Op::Delete(key)],
                        otherwise: Vec::new(),
                    })
                    .await?;
                if res.succeeded {
                    n += 1;
                }
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::machine::Act;
    use crate::model::{Action, Kind, Level, Mode, Subject};
    use sprig_common::ids::DcId;

    fn p(to: &str) -> Proposal {
        Proposal {
            kind: Kind::FailoverCluster,
            subject: Subject {
                dc: Some(DcId::new("mum1").unwrap()),
                ..Subject::default()
            },
            target: "mum1-c1".into(),
            action: Action {
                text: format!("promote {to}"),
                machine: serde_json::json!({ "to": to }),
            },
            evidence: serde_json::json!({}),
            score: 1.0,
            summary: "writer down".into(),
            emergency: false,
        }
    }

    #[tokio::test]
    async fn tier_store_raises_refreshes_supersedes_and_writes_by_version() {
        let dc = DcId::new("mum1").unwrap();
        let meta: Arc<dyn MetaStore> = Arc::new(sprig_meta::MemStore::new(Tier::Dc(dc.clone())));
        let s = EtcdRecStore::new(meta, Tier::Dc(dc.clone()));
        let pol = Policy::fixed(Mode::Approve, Level::Dc);
        let a = s.raise(&p("r1"), &pol, &[], 10).await.unwrap();
        assert_eq!(a.verdict, Verdict::Wait(RecId::Dc(dc.clone(), 1)));
        let b = s.raise(&p("r1"), &pol, &[], 20).await.unwrap();
        assert_eq!(b.verdict, Verdict::Wait(RecId::Dc(dc.clone(), 1)));
        let c = s.raise(&p("r2"), &pol, &[], 30).await.unwrap();
        assert_eq!(c.verdict, Verdict::Wait(RecId::Dc(dc.clone(), 2)));
        assert_eq!(c.superseded.len(), 1);
        assert_eq!(s.get(1).await.unwrap().unwrap().state, State::Superseded);
        let (r, _) = crate::store::modify(&s, &RecId::Dc(dc.clone(), 2), |r| {
            crate::machine::act(
                r,
                &Act::Approve {
                    reason: String::new(),
                    window: None,
                },
                "op",
                1,
                2.0,
                40,
            )
            .map(Some)
        })
        .await
        .unwrap();
        assert_eq!(r.state, State::Approved);
        let d = s.raise(&p("r2"), &pol, &[], 50).await.unwrap();
        assert!(d.verdict.proceed());
        assert_eq!(
            s.list(&Filter {
                all: true,
                ..Filter::default()
            })
            .await
            .unwrap()
            .len(),
            2
        );
        assert_eq!(s.purge(i64::MAX).await.unwrap(), 1);
    }
}
