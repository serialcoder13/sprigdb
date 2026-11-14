//! Per-collection latencies and per-index use:
//! what `$collStats.latencyStats` and `$indexStats.accesses` report, kept
//! in the gateway's memory since its start, as `mongod` keeps them since
//! its own.
//!
//! - **Latencies:** the gateway records each MongoDB command on a
//!   collection after it runs: reads (`find`, `aggregate`, `count`,
//!   `distinct`, `getMore`), writes (`insert`, `update`, `delete`,
//!   `findAndModify`), other commands, and commands inside a transaction
//!   as transactions. Histograms have power-of-two buckets.
//! - **Index use:** the engine notes, for each read and write it plans,
//!   the index MariaDB can use for its filter: the one with the most
//!   leading key fields constrained by the filter's top-level fields (or,
//!   without one, the one whose first field leads the sort). It is the
//!   index the plan is built for, not what `EXPLAIN` reports.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use bson::{Bson, DateTime, Document, doc};

use crate::meta::CollMeta;

/// A latency class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Reads.
    Reads,
    /// Writes.
    Writes,
    /// Other commands on the collection.
    Commands,
    /// Commands inside a multi-document transaction.
    Transactions,
}

const BUCKETS: usize = 40;

#[derive(Debug, Clone)]
struct Lat {
    ops: u64,
    micros: u64,
    hist: [u64; BUCKETS],
}

impl Default for Lat {
    fn default() -> Self {
        Self {
            ops: 0,
            micros: 0,
            hist: [0; BUCKETS],
        }
    }
}

type NsKey = (u64, String, String);

#[derive(Default)]
struct Tables {
    lat: HashMap<NsKey, [Lat; 4]>,
    idx: HashMap<(u64, String, String, String), (u64, DateTime)>,
}

fn tables() -> std::sync::MutexGuard<'static, Tables> {
    static T: OnceLock<Mutex<Tables>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(Tables::default()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The time counting started.
#[must_use]
pub fn since() -> DateTime {
    static S: OnceLock<DateTime> = OnceLock::new();
    *S.get_or_init(DateTime::now)
}

/// Records one command on `db.coll` taking `micros`.
pub fn record_latency(tenant: u64, db: &str, coll: &str, class: Class, micros: u64) {
    let _ = since();
    let mut t = tables();
    let e = t
        .lat
        .entry((tenant, db.to_owned(), coll.to_owned()))
        .or_default();
    let l = &mut e[class as usize];
    l.ops += 1;
    l.micros = l.micros.saturating_add(micros);
    let b = usize::try_from(64 - micros.max(1).leading_zeros() - 1).unwrap_or(0);
    l.hist[b.min(BUCKETS - 1)] += 1;
}

/// `latencyStats` of `db.coll`, MongoDB's shape.
#[must_use]
pub fn latency_stats(tenant: u64, db: &str, coll: &str, histograms: bool) -> Document {
    let t = tables();
    let e = t.lat.get(&(tenant, db.to_owned(), coll.to_owned()));
    let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    let mut out = Document::new();
    for (i, k) in ["reads", "writes", "commands", "transactions"]
        .iter()
        .enumerate()
    {
        let l = e.map(|e| e[i].clone()).unwrap_or_default();
        let mut d = doc! {"latency": n(l.micros), "ops": n(l.ops), "queryableEncryptionLatencyMicros": 0i64};
        if histograms {
            let h: Vec<Bson> = l
                .hist
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .map(|(b, c)| Bson::Document(doc! {"micros": n(1u64 << b), "count": n(*c)}))
                .collect();
            d.insert("histogram", h);
        }
        out.insert(*k, d);
    }
    out
}

/// Notes one use of `index` on `db.coll`.
pub fn record_index(tenant: u64, db: &str, coll: &str, index: &str) {
    let mut t = tables();
    t.idx
        .entry((tenant, db.to_owned(), coll.to_owned(), index.to_owned()))
        .or_insert((0, DateTime::now()))
        .0 += 1;
}

/// `accesses` of `index` on `db.coll`: ops and since when.
#[must_use]
pub fn index_accesses(tenant: u64, db: &str, coll: &str, index: &str) -> (i64, DateTime) {
    let t = tables();
    t.idx
        .get(&(tenant, db.to_owned(), coll.to_owned(), index.to_owned()))
        .map_or((0, since()), |(n, s)| {
            (i64::try_from(*n).unwrap_or(i64::MAX), *s)
        })
}

/// Forgets a dropped collection's counters (or one index's).
pub fn forget(tenant: u64, db: &str, coll: &str, index: Option<&str>) {
    let mut t = tables();
    if index.is_none() {
        t.lat.remove(&(tenant, db.to_owned(), coll.to_owned()));
    }
    t.idx.retain(|(tn, d, c, i), _| {
        !(*tn == tenant && d == db && c == coll && index.is_none_or(|x| x == i))
    });
}

/// The top-level field paths `filter` constrains (through `$and`).
fn constrained(filter: &Document, out: &mut Vec<String>) {
    for (k, v) in filter {
        if k == "$and" {
            if let Bson::Array(a) = v {
                for x in a {
                    if let Bson::Document(d) = x {
                        constrained(d, out);
                    }
                }
            }
        } else if !k.starts_with('$') {
            // `{$exists: false}` and `$ne`/`$nin` alone can't seek.
            let seeks = match v {
                Bson::Document(d) if d.keys().next().is_some_and(|o| o.starts_with('$')) => d
                    .keys()
                    .any(|o| !matches!(o.as_str(), "$ne" | "$nin" | "$exists" | "$not")),
                _ => true,
            };
            if seeks {
                out.push(k.clone());
            }
        }
    }
}

/// The index a read or write of `m` with `filter` (and `sort`) is planned
/// for, if any.
#[must_use]
pub fn planned_index<'m>(
    m: &'m CollMeta,
    filter: &Document,
    sort: Option<&Document>,
) -> Option<&'m str> {
    let keys: Vec<(&str, Vec<&str>)> = m
        .indexes
        .iter()
        .map(|ix| {
            (
                ix.name.as_str(),
                ix.key.iter().map(|(p, _)| p.as_str()).collect(),
            )
        })
        .collect();
    pick(&keys, filter, sort).map(|i| keys[i].0)
}

/// [`planned_index`] over `(name, key paths)`.
fn pick(
    indexes: &[(&str, Vec<&str>)],
    filter: &Document,
    sort: Option<&Document>,
) -> Option<usize> {
    let mut fields = Vec::new();
    constrained(filter, &mut fields);
    let mut best: Option<(usize, usize)> = None;
    for (i, (_, key)) in indexes.iter().enumerate() {
        let lead = key
            .iter()
            .take_while(|p| fields.iter().any(|f| f == *p))
            .count();
        if lead > 0 && best.is_none_or(|(_, b)| lead > b) {
            best = Some((i, lead));
        }
    }
    if let Some((i, _)) = best {
        return Some(i);
    }
    let first = sort.and_then(|s| s.keys().next())?;
    indexes
        .iter()
        .position(|(_, key)| key.first().is_some_and(|p| p == first))
}

/// Notes the planned index of a read or write.
pub fn note(
    tenant: u64,
    db: &str,
    coll: &str,
    m: &CollMeta,
    filter: &Document,
    sort: Option<&Document>,
) {
    if let Some(ix) = planned_index(m, filter, sort) {
        record_index(tenant, db, coll, ix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latencies_and_buckets() {
        record_latency(77, "d", "c", Class::Reads, 3);
        record_latency(77, "d", "c", Class::Reads, 1000);
        record_latency(77, "d", "c", Class::Writes, 10);
        let s = latency_stats(77, "d", "c", true);
        let r = s.get_document("reads").unwrap();
        assert_eq!(r.get_i64("ops").unwrap(), 2);
        assert_eq!(r.get_i64("latency").unwrap(), 1003);
        let h = r.get_array("histogram").unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h[1].as_document().unwrap().get_i64("micros").unwrap(), 512);
        assert_eq!(s.get_document("writes").unwrap().get_i64("ops").unwrap(), 1);
        forget(77, "d", "c", None);
        assert_eq!(
            latency_stats(77, "d", "c", false)
                .get_document("reads")
                .unwrap()
                .get_i64("ops")
                .unwrap(),
            0
        );
    }

    #[test]
    fn the_planned_index_has_the_longest_constrained_prefix() {
        let ix: Vec<(&str, Vec<&str>)> = vec![
            ("_id_", vec!["_id"]),
            ("a_1", vec!["a"]),
            ("a_1_b_1", vec!["a", "b"]),
        ];
        let name = |f: Document, s: Option<Document>| pick(&ix, &f, s.as_ref()).map(|i| ix[i].0);
        assert_eq!(name(doc! {"a": 1, "b": {"$gt": 2}}, None), Some("a_1_b_1"));
        assert_eq!(name(doc! {"$and": [{"a": 1}]}, None), Some("a_1"));
        assert_eq!(name(doc! {"_id": 5}, None), Some("_id_"));
        assert_eq!(name(doc! {"a": {"$ne": 1}}, None), None);
        assert_eq!(name(doc! {}, Some(doc! {"a": -1})), Some("a_1"));
    }
}
