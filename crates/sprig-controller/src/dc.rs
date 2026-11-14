//! Data-center records: `sprigctl dc register|list|show`.
//!
//! `register` writes `dcs/<dc_id>` as `joining`, guarded by `Missing`; the
//! global leader's `dc_registry` loop marks it `active` once
//! its controllers hold the DC lease and latency rows exist both ways. The
//! first DC is written `active` by bootstrap ([`sprig_meta::bootstrap`]).
//!
//! These functions are what the Admin API's `DataCenters` calls run
//! ([`crate::admin`]); `sprigctl dc` reaches them through that API.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::elect::{self, Leader};
use sprig_meta::{
    Cmp, Consistency, ListOpts, MetaStore, Op, OpResult, Record, Revision, Tier, Txn,
};

/// A DC's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcKind {
    /// Holds data and serves clients.
    Full,
    /// Only votes and observes (in DC failover).
    Witness,
}

impl DcKind {
    /// The name in records.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            DcKind::Full => "full",
            DcKind::Witness => "witness",
        }
    }
}

impl std::str::FromStr for DcKind {
    type Err = SprigError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "full" => Ok(DcKind::Full),
            "witness" => Ok(DcKind::Witness),
            _ => Err(SprigError::new(
                SprigCode::ConfigInvalid,
                format!("DC kind {s:?}: expected full or witness"),
            )),
        }
    }
}

/// What to register.
#[derive(Debug, Clone, PartialEq)]
pub struct DcRegistration {
    /// The new DC.
    pub dc: DcId,
    /// Its kind.
    pub kind: DcKind,
    /// Country, jurisdiction and other tags.
    pub tags: Map<String, Value>,
    /// Whether a global etcd voter runs there.
    pub global_voter: bool,
}

/// What [`register`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum Registered {
    /// This call wrote the record, as `joining`.
    Created(DcRecord),
    /// The DC was already registered; nothing changed.
    Exists(DcRecord),
}

/// A DC as `sprigctl dc show` prints it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcView {
    /// The record.
    #[serde(flatten)]
    pub record: DcRecord,
    /// The record's mod revision.
    pub mod_revision: Revision,
    /// The DC's current leader, when its tier is reachable from here
    /// (always in single-etcd mode).
    pub leader: Option<LeaderView>,
}

/// A leader as `sprigctl dc show` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderView {
    /// The leading controller.
    pub holder: String,
    /// Its build.
    pub build: String,
    /// Its fencing token.
    pub token: Revision,
}

impl From<Leader> for LeaderView {
    fn from(l: Leader) -> Self {
        Self {
            holder: l.record.holder,
            build: l.record.build,
            token: l.token,
        }
    }
}

fn check_global(global: &dyn MetaStore) -> Result<()> {
    if global.tier() == &Tier::Global {
        Ok(())
    } else {
        Err(SprigError::internal("DC records live in the global tier"))
    }
}

/// Registers a DC as `joining`, once.
///
/// # Errors
/// Internal if `global` isn't the global tier; the store's errors.
pub async fn register(global: &dyn MetaStore, reg: DcRegistration) -> Result<Registered> {
    check_global(global)?;
    let key = Tier::Global.key(&format!("dcs/{}", reg.dc))?;
    let rec = DcRecord {
        dc_id: reg.dc.clone(),
        kind: reg.kind.as_str().to_owned(),
        state: "joining".to_owned(),
        dc_epoch: 0,
        tags: reg.tags,
        global_voter: reg.global_voter,
    };
    let r = global
        .txn(Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key: key.clone(),
                value: Record::new(rec.clone()).encode()?,
                lease: None,
            }],
            otherwise: vec![Op::Get(key)],
        })
        .await?;
    if r.succeeded {
        tracing::info!(dc = %reg.dc, "DC registered as joining");
        return Ok(Registered::Created(rec));
    }
    match r.results.into_iter().next() {
        Some(OpResult::Get(Some(kv))) => Ok(Registered::Exists(
            Record::<DcRecord>::decode(&kv.value)?.body,
        )),
        // Deleted in between: let the caller try again.
        _ => Err(SprigError::new(
            SprigCode::MetaUnavailable,
            format!("dcs/{} changed during registration; try again", reg.dc),
        )),
    }
}

/// Every registered DC, by id.
///
/// # Errors
/// Internal if `global` isn't the global tier; the store's errors.
pub async fn list(global: &dyn MetaStore) -> Result<Vec<DcRecord>> {
    check_global(global)?;
    let prefix = Tier::Global.key("dcs/")?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = global.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            match Record::<DcRecord>::decode(&kv.value) {
                Ok(r) => out.push(r.body),
                Err(e) => tracing::warn!(key = %kv.key, error = %e, "skipping a bad DC record"),
            }
        }
        if !page.more {
            return Ok(out);
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
}

/// One DC with its current leader. `dc_tier` reads the DC's `leader` key;
/// pass `None` when that tier isn't reachable.
///
/// # Errors
/// Internal if `global` isn't the global tier; the store's errors.
pub async fn show(
    global: &dyn MetaStore,
    dc_tier: Option<Arc<dyn MetaStore>>,
    dc: &DcId,
) -> Result<Option<DcView>> {
    check_global(global)?;
    let key = Tier::Global.key(&format!("dcs/{dc}"))?;
    let Some(kv) = global.get(&key, Consistency::Linearizable).await? else {
        return Ok(None);
    };
    let record = Record::<DcRecord>::decode(&kv.value)?.body;
    let leader = match dc_tier {
        Some(store) => {
            let lk = store.tier().key("leader")?;
            elect::current_leader(&*store, &lk)
                .await?
                .map(LeaderView::from)
        }
        None => None,
    };
    Ok(Some(DcView {
        record,
        mod_revision: kv.mod_revision,
        leader,
    }))
}

/// Parses `key=value` tags.
///
/// # Errors
/// 39998 `ER_SPRIG_CONFIG_INVALID` for a tag without `=` or with an empty
/// key.
pub fn parse_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Result<Map<String, Value>> {
    let mut out = Map::new();
    for t in tags {
        match t.split_once('=') {
            Some((k, v)) if !k.trim().is_empty() => {
                out.insert(k.trim().to_owned(), Value::String(v.trim().to_owned()));
            }
            _ => {
                return Err(SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("tag {t:?}: expected key=value"),
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use sprig_meta::MemStore;

    use super::*;

    #[tokio::test]
    async fn register_list_show() {
        let g = MemStore::new(Tier::Global);
        let ams1 = DcId::new("ams1").unwrap();
        let reg = DcRegistration {
            dc: ams1.clone(),
            kind: DcKind::Full,
            tags: parse_tags(["jurisdiction=EU", "country = NL"]).unwrap(),
            global_voter: false,
        };
        let Registered::Created(rec) = register(&g, reg.clone()).await.unwrap() else {
            panic!("expected a new record");
        };
        assert_eq!(rec.state, "joining");
        assert_eq!(rec.tags["country"], "NL");
        assert!(matches!(
            register(&g, reg).await.unwrap(),
            Registered::Exists(r) if r.state == "joining"
        ));
        let all = list(&g).await.unwrap();
        assert_eq!(all.len(), 1);

        let dc_tier: Arc<dyn MetaStore> = Arc::new(g.with_tier(Tier::Dc(ams1.clone())));
        let v = show(&g, Some(dc_tier), &ams1).await.unwrap().unwrap();
        assert_eq!(
            (v.record.dc_id.as_str(), v.leader.is_none()),
            ("ams1", true)
        );
        let missing = DcId::new("fra1").unwrap();
        assert!(show(&g, None, &missing).await.unwrap().is_none());

        assert!(parse_tags(["noequals"]).is_err());
        assert!(parse_tags(["=x"]).is_err());
        assert!("edge".parse::<DcKind>().is_err());
        assert!(list(&g.with_tier(Tier::Dc(ams1))).await.is_err());
    }
}
