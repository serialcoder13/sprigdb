//! Where copies live (chapter 39, "Placement of copies", "Partitions and
//! copies"; build step 5; R5 round 6).
//!
//! Records in the DC tier:
//!
//! ```text
//! search/indexes/<tenant>/<index>       IndexRecord: the definition, its analyzers,
//!                                       the generation, how many copies (search.copies)
//! search/copies/<tenant>/<index>/<n>    CopyRecord: copy n's worker, address, state,
//!                                       generation and applied positions
//! ```
//!
//! ```text
//! gateway, on first use of an index (and when its definition changes):
//!   put IndexRecord (unless equal)
//! controller (DC leader), search_placement loop, every 2 s:
//!   each IndexRecord: copies 0..copies-1 without a live search_copy job → submit one
//!     (class search: only workers with the search role take it; spread by index,
//!     so the dispatcher puts copies of one index on different workers when it can);
//!   live jobs whose index record is gone, or whose n ≥ copies → cancelled
//! worker (search role), search_copy job (tenant, index, n):
//!   a copy of the record's generation: files on disk → resume; else from a ready
//!     peer (CopySegments), else from a snapshot, else built from MariaDB through a
//!     gateway (SearchSeed); CopyRecord written every second while it runs
//! gateway query: a ready copy of the index's generation on a live worker; another
//!   on failure; none ready → wait (39824) ; none at all → 39825
//! ```
//!
//! Partitions are the index's key-hash buckets (decision S2), so a copy is
//! the whole index and a worker loss costs one copy, served meanwhile by
//! the other.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};
use sprig_meta::{Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use crate::analyze::CustomAnalyzer;
use crate::def::IndexDef;

/// The job kind of a copy.
pub const JOB_KIND: &str = "search_copy";

/// `search/indexes/<tenant>/<index>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRecord {
    /// The tenant.
    pub tenant: u64,
    /// The index (`objects.object_id`).
    pub index: u64,
    /// The definition (its generation names the copies' files).
    pub def: IndexDef,
    /// The tenant's search analyzers the definition may name.
    #[serde(default)]
    pub analyzers: Vec<CustomAnalyzer>,
    /// Copies wanted (`search.copies`).
    pub copies: u32,
}

impl Versioned for IndexRecord {
    const V: u32 = 1;
}

/// A copy's state as its worker reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyRecord {
    /// The tenant.
    pub tenant: u64,
    /// The index.
    pub index: u64,
    /// Which copy (0..copies).
    pub copy: u32,
    /// The definition's generation the copy holds.
    pub generation: u32,
    /// The worker's registration id.
    pub worker_id: String,
    /// Its `SearchPartition` address, `host:port`.
    pub addr: String,
    /// `seeding`, `ready`, `failed` or `stopped`.
    pub state: String,
    /// Each cluster's applied position.
    #[serde(default)]
    pub applied: BTreeMap<String, String>,
    /// The job that holds it.
    pub job_id: String,
    /// When written (Unix ms).
    pub updated_ms: u64,
    /// How the copy was filled: `files`, `peer`, `snapshot` or `mariadb`.
    #[serde(default)]
    pub seeded_from: String,
}

impl Versioned for CopyRecord {
    const V: u32 = 1;
}

/// `search/indexes/<tenant>/<index>`.
#[must_use]
pub fn index_rel(tenant: u64, index: u64) -> String {
    format!("search/indexes/{tenant}/{index}")
}

/// `search/copies/<tenant>/<index>/`.
#[must_use]
pub fn copies_rel(tenant: u64, index: u64) -> String {
    format!("search/copies/{tenant}/{index}/")
}

/// `search/copies/<tenant>/<index>/<copy>`.
#[must_use]
pub fn copy_rel(tenant: u64, index: u64, copy: u32) -> String {
    format!("search/copies/{tenant}/{index}/{copy}")
}

/// The spread key of an index's copy jobs (one worker per copy).
#[must_use]
pub fn spread(tenant: u64, index: u64) -> String {
    format!("search:{tenant}:{index}")
}

async fn list_all(store: &Arc<dyn MetaStore>, rel: &str) -> Result<Vec<sprig_meta::KeyValue>> {
    let prefix = store.tier().key(rel)?;
    let mut out = Vec::new();
    let mut opts = ListOpts {
        limit: 1000,
        ..ListOpts::default()
    };
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        let more = page.more;
        let last = page.kvs.last().map(|kv| kv.key.clone());
        out.extend(page.kvs);
        match (more, last) {
            (true, Some(k)) => {
                opts.after = Some(k);
                opts.at = Some(page.revision);
            }
            _ => return Ok(out),
        }
    }
}

/// The index's record.
///
/// # Errors
/// The store's.
pub async fn get_index(
    store: &Arc<dyn MetaStore>,
    tenant: u64,
    index: u64,
) -> Result<Option<IndexRecord>> {
    let key = store.tier().key(&index_rel(tenant, index))?;
    match store.get(&key, Consistency::Linearizable).await? {
        Some(kv) => Ok(Some(Record::<IndexRecord>::decode(&kv.value)?.body)),
        None => Ok(None),
    }
}

/// Writes `rec` unless the stored record equals it; whether it wrote.
///
/// # Errors
/// The store's.
pub async fn put_index(store: &Arc<dyn MetaStore>, rec: &IndexRecord) -> Result<bool> {
    if get_index(store, rec.tenant, rec.index).await?.as_ref() == Some(rec) {
        return Ok(false);
    }
    let key = store.tier().key(&index_rel(rec.tenant, rec.index))?;
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: Record::new(rec.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(true)
}

/// Removes the index's record and its copies' records (a dropped index).
///
/// # Errors
/// The store's.
pub async fn drop_index(store: &Arc<dyn MetaStore>, tenant: u64, index: u64) -> Result<()> {
    let mut ops = vec![Op::Delete(store.tier().key(&index_rel(tenant, index))?)];
    for kv in list_all(store, &copies_rel(tenant, index)).await? {
        ops.push(Op::Delete(kv.key));
    }
    store
        .txn(Txn {
            when: Vec::new(),
            then: ops,
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Every index record of the DC.
///
/// # Errors
/// The store's.
pub async fn list_indexes(store: &Arc<dyn MetaStore>) -> Result<Vec<IndexRecord>> {
    Ok(list_all(store, "search/indexes/")
        .await?
        .iter()
        .filter_map(|kv| Record::<IndexRecord>::decode(&kv.value).ok())
        .map(|r| r.body)
        .collect())
}

/// The copies' records of an index.
///
/// # Errors
/// The store's.
pub async fn list_copies(
    store: &Arc<dyn MetaStore>,
    tenant: u64,
    index: u64,
) -> Result<Vec<CopyRecord>> {
    Ok(list_all(store, &copies_rel(tenant, index))
        .await?
        .iter()
        .filter_map(|kv| Record::<CopyRecord>::decode(&kv.value).ok())
        .map(|r| r.body)
        .collect())
}

/// Writes a copy's record.
///
/// # Errors
/// The store's.
pub async fn put_copy(store: &Arc<dyn MetaStore>, rec: &CopyRecord) -> Result<()> {
    let key = store
        .tier()
        .key(&copy_rel(rec.tenant, rec.index, rec.copy))?;
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: Record::new(rec.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Removes copy `copy`'s record if job `job_id` still holds it.
///
/// # Errors
/// The store's.
pub async fn drop_copy(
    store: &Arc<dyn MetaStore>,
    tenant: u64,
    index: u64,
    copy: u32,
    job_id: &str,
) -> Result<()> {
    let key = store.tier().key(&copy_rel(tenant, index, copy))?;
    let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
        return Ok(());
    };
    let rec = Record::<CopyRecord>::decode(&kv.value)?.body;
    if rec.job_id != job_id {
        return Ok(());
    }
    store
        .txn(Txn {
            when: vec![sprig_meta::Cmp::ModRev(key.clone(), kv.mod_revision)],
            then: vec![Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// The order a gateway tries an index's copies in: ready copies of
/// `generation` on `live` workers, the one most recently written first
/// (ties by copy number), then copies still seeding. Copies of another
/// generation, or of workers that are gone, are left out.
#[must_use]
pub fn query_order(
    copies: &[CopyRecord],
    generation: u32,
    live: &dyn Fn(&str) -> bool,
) -> (Vec<CopyRecord>, Vec<CopyRecord>) {
    let mut ready: Vec<CopyRecord> = Vec::new();
    let mut seeding: Vec<CopyRecord> = Vec::new();
    for c in copies {
        if c.generation != generation || !live(&c.worker_id) {
            continue;
        }
        match c.state.as_str() {
            "ready" => ready.push(c.clone()),
            "seeding" => seeding.push(c.clone()),
            _ => {}
        }
    }
    ready.sort_by(|a, b| b.updated_ms.cmp(&a.updated_ms).then(a.copy.cmp(&b.copy)));
    seeding.sort_by_key(|c| c.copy);
    (ready, seeding)
}

/// An index record's definition as the copies need it (errors are
/// internal: a record a gateway wrote).
///
/// # Errors
/// Internal for a record without an index id.
pub fn checked(rec: IndexRecord) -> Result<IndexRecord> {
    if rec.index == 0 || rec.def.index_id != rec.index {
        return Err(SprigError::internal(format!(
            "search index record {}/{} names index {}",
            rec.tenant, rec.index, rec.def.index_id
        )));
    }
    Ok(rec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy(n: u32, generation: u32, worker: &str, state: &str, at: u64) -> CopyRecord {
        CopyRecord {
            tenant: 1,
            index: 2,
            copy: n,
            generation,
            worker_id: worker.into(),
            addr: format!("127.0.0.1:{}", 7400 + n),
            state: state.into(),
            applied: BTreeMap::new(),
            job_id: format!("j{n}"),
            updated_ms: at,
            seeded_from: String::new(),
        }
    }

    #[test]
    fn query_order_prefers_ready_live_copies_of_the_generation() {
        let copies = vec![
            copy(0, 3, "w1", "ready", 100),
            copy(1, 3, "w2", "ready", 200),
            copy(2, 3, "w3", "seeding", 300),
            copy(3, 2, "w4", "ready", 400),
            copy(4, 3, "gone", "ready", 500),
            copy(5, 3, "w5", "failed", 600),
        ];
        let live = |w: &str| w != "gone";
        let (ready, seeding) = query_order(&copies, 3, &live);
        assert_eq!(
            ready.iter().map(|c| c.copy).collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert_eq!(seeding.iter().map(|c| c.copy).collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn keys() {
        assert_eq!(index_rel(7, 9), "search/indexes/7/9");
        assert_eq!(copy_rel(7, 9, 1), "search/copies/7/9/1");
        assert!(copy_rel(7, 9, 1).starts_with(&copies_rel(7, 9)));
        assert_eq!(spread(7, 9), "search:7:9");
    }
}
