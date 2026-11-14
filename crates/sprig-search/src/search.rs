//! A search index's partitions, and searching them with global BM25.
//!
//! ```text
//! search(query, k):
//!   build the query once (fuzzy and prefix terms expanded over every partition)
//!   global: N := Σ N_p, T[f] := Σ T_p[f], n[t] := Σ n_p[t] over the partitions
//!   each partition scores its top k through a statistics provider returning
//!     the global values
//!   merge by (score desc, partition, key)
//! ```
//!
//! Each document is in exactly one partition, so the sums are the
//! statistics one unsharded index would have, and a document scores the
//! same in any partition. **Partitions here are key-hash buckets**, not
//! the object's shards: every copy holds all partitions of its index,
//! so a split or a move of a shard changes nothing in the index; the feed carries the rows.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use sprig_common::{Result, SprigCode, SprigError};
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{Bm25StatisticsProvider, Query};
use tantivy::schema::{Field, Value};
use tantivy::{DocAddress, Searcher, TantivyDocument, Term};

use crate::build::{Builder, Expander, Expansion, TermBase};
use crate::partition::Partition;
use crate::query::SearchQuery;
use crate::schema::{IndexSchema, Key, Record};

/// How scores are computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Scoring {
    /// Statistics summed over every partition (`search.scoring = global`).
    Global,
    /// Each partition's own statistics (`local`).
    Local,
}

/// One hit.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Hit {
    /// BM25 score.
    pub score: f32,
    /// The partition.
    pub part: u32,
    /// The key.
    pub key: Key,
}

fn order(a: &Hit, b: &Hit) -> std::cmp::Ordering {
    b.score
        .total_cmp(&a.score)
        .then(a.part.cmp(&b.part))
        .then_with(|| a.key.cmp(&b.key))
}

fn terr(e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("search: {e}"))
}

/// FNV-1a, for placing keys in partitions.
#[must_use]
pub fn key_hash(k: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in k {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Statistics of one partition for a query's terms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartStats {
    /// Live documents.
    pub num_docs: u64,
    /// Tokens per field.
    pub tokens: HashMap<Field, u64>,
    /// Documents per term.
    pub doc_freq: HashMap<Term, u64>,
}

/// Statistics summed over partitions, served to Tantivy's BM25.
#[derive(Debug, Default)]
pub struct GlobalStats {
    sum: PartStats,
    /// Searchers for anything the query's terms didn't list.
    searchers: Vec<Searcher>,
    extra: Mutex<HashMap<Term, u64>>,
}

impl GlobalStats {
    /// The sum of `parts`; `searchers` answer what they don't hold.
    #[must_use]
    pub fn sum(parts: &[PartStats], searchers: Vec<Searcher>) -> Self {
        let mut s = PartStats::default();
        for p in parts {
            s.num_docs += p.num_docs;
            for (f, n) in &p.tokens {
                *s.tokens.entry(*f).or_default() += n;
            }
            for (t, n) in &p.doc_freq {
                *s.doc_freq.entry(t.clone()).or_default() += n;
            }
        }
        Self {
            sum: s,
            searchers,
            extra: Mutex::new(HashMap::new()),
        }
    }

    /// The summed statistics.
    #[must_use]
    pub fn stats(&self) -> &PartStats {
        &self.sum
    }
}

impl Bm25StatisticsProvider for GlobalStats {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        if let Some(n) = self.sum.tokens.get(&field) {
            return Ok(*n);
        }
        let mut n = 0;
        for s in &self.searchers {
            n += s.total_num_tokens(field)?;
        }
        Ok(n)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.sum.num_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if let Some(n) = self.sum.doc_freq.get(term) {
            return Ok(*n);
        }
        let mut cache = self
            .extra
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = cache.get(term) {
            return Ok(*n);
        }
        let mut n = 0;
        for s in &self.searchers {
            n += s.doc_freq(term)?;
        }
        cache.insert(term.clone(), n);
        Ok(n)
    }
}

/// A partition's statistics for `query`'s terms (the `Stats` RPC).
///
/// # Errors
/// Tantivy's.
pub fn part_stats(searcher: &Searcher, query: &dyn Query) -> Result<PartStats> {
    let mut terms: Vec<Term> = Vec::new();
    query.query_terms(&mut |t, _| terms.push(t.clone()));
    let mut s = PartStats {
        num_docs: searcher.num_docs(),
        ..PartStats::default()
    };
    for t in terms {
        let f = t.field();
        if let std::collections::hash_map::Entry::Vacant(e) = s.tokens.entry(f) {
            e.insert(searcher.total_num_tokens(f).map_err(terr)?);
        }
        let n = searcher.doc_freq(&t).map_err(terr)?;
        s.doc_freq.insert(t, n);
    }
    Ok(s)
}

/// Bounded Levenshtein distance (`None` above `max`).
fn within(a: &str, b: &str, max: usize) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > max {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        let mut best = cur[0];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
            best = best.min(cur[j + 1]);
        }
        if best > max {
            return false;
        }
        prev = cur;
    }
    prev[b.len()] <= max
}

/// Expands over a set of searchers (every partition of the index).
struct Over<'a>(&'a [Searcher]);

impl Expander for Over<'_> {
    fn expand(&self, base: &TermBase, how: &Expansion, max: usize) -> Result<Vec<String>> {
        let pre = base.value_prefix();
        let mut found: BTreeMap<String, u64> = BTreeMap::new();
        for s in self.0 {
            for seg in s.segment_readers() {
                let inv = seg.inverted_index(base.field).map_err(terr)?;
                let dict = inv.terms();
                let start: Vec<u8> = match how {
                    Expansion::Prefix(p) => [pre.as_slice(), p.as_bytes()].concat(),
                    Expansion::Fuzzy(..) => pre.clone(),
                };
                let mut st = dict.range().ge(&start).into_stream().map_err(terr)?;
                while st.advance() {
                    let k = st.key();
                    if !k.starts_with(&pre) {
                        break;
                    }
                    let Ok(text) = std::str::from_utf8(&k[pre.len()..]) else {
                        continue;
                    };
                    let keep = match how {
                        Expansion::Prefix(p) => {
                            if !text.starts_with(p.as_str()) {
                                break;
                            }
                            true
                        }
                        Expansion::Fuzzy(w, d) => within(w, text, usize::from(*d)),
                    };
                    if keep {
                        *found.entry(text.to_owned()).or_default() +=
                            u64::from(st.value().doc_freq);
                    }
                }
            }
        }
        // The most frequent terms, as Lucene's top-terms rewrite keeps.
        let mut v: Vec<(String, u64)> = found.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(max.max(1));
        let mut out: Vec<String> = v.into_iter().map(|(t, _)| t).collect();
        out.sort();
        Ok(out)
    }
}

/// A search index's partitions.
#[derive(Debug)]
pub struct SearchIndex {
    /// The schema.
    pub schema: IndexSchema,
    parts: Vec<Partition>,
}

/// One facet bucket.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Bucket {
    /// The value (keyword) or the lower boundary (numbers).
    pub value: serde_json::Value,
    /// Matching documents.
    pub count: u64,
}

impl SearchIndex {
    /// Opens (or creates) `partitions` partitions under `dir`
    /// (`p<n>` subdirectories), or in memory.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn open(schema: IndexSchema, dir: Option<&Path>, writer_bytes: usize) -> Result<Self> {
        let n = schema.def.partitions.max(1);
        let mut parts = Vec::new();
        for i in 0..n {
            let d = dir.map(|d| d.join(format!("p{i}")));
            parts.push(Partition::open(i, &schema, d.as_deref(), writer_bytes)?);
        }
        Ok(Self { schema, parts })
    }

    /// The partitions.
    #[must_use]
    pub fn partitions(&self) -> &[Partition] {
        &self.parts
    }

    /// The partition of a key.
    #[must_use]
    pub fn part_of(&self, key: &Key) -> usize {
        #[allow(clippy::cast_possible_truncation)]
        let i = (key_hash(&key.0) % self.parts.len() as u64) as usize;
        i
    }

    /// Indexes `r` (replacing the key's document).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn upsert(&self, r: &Record) -> Result<()> {
        self.parts[self.part_of(r.key())].index(&self.schema, r)
    }

    /// Removes a key.
    pub fn delete(&self, key: &Key) {
        self.parts[self.part_of(key)].delete(key);
    }

    /// Removes everything.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn delete_all(&self) -> Result<()> {
        for p in &self.parts {
            p.delete_all()?;
        }
        Ok(())
    }

    /// Commits every partition with `payload`.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn commit(&self, payload: &str) -> Result<()> {
        for p in &self.parts {
            p.commit(payload)?;
        }
        Ok(())
    }

    /// The oldest payload of the partitions (where to resume), `None` if
    /// any partition has none.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn payloads(&self) -> Result<Vec<Option<String>>> {
        self.parts.iter().map(Partition::payload).collect()
    }

    /// Live documents.
    #[must_use]
    pub fn num_docs(&self) -> u64 {
        self.parts.iter().map(Partition::num_docs).sum()
    }

    fn searchers(&self) -> Vec<Searcher> {
        self.parts.iter().map(Partition::searcher).collect()
    }

    /// The Tantivy query of `q` over these searchers.
    ///
    /// # Errors
    /// As [`Builder::build`].
    pub fn compile(
        &self,
        q: &SearchQuery,
        searchers: &[Searcher],
        max_expansions: usize,
    ) -> Result<Box<dyn Query>> {
        let ex = Over(searchers);
        Builder::new(&self.schema, &ex, max_expansions).build(q)
    }

    fn key_of(&self, s: &Searcher, a: DocAddress) -> Result<Key> {
        let d: TantivyDocument = s.doc(a).map_err(terr)?;
        d.get_first(self.schema.key)
            .and_then(|v| v.as_bytes())
            .map(|b| Key(b.to_vec()))
            .ok_or_else(|| SprigError::internal("a search document without _key"))
    }

    /// The best `k` hits.
    ///
    /// # Errors
    /// 39822 and the other errors of [`Builder::build`]; Tantivy's.
    pub fn top(
        &self,
        q: &SearchQuery,
        k: usize,
        scoring: Scoring,
        max_expansions: usize,
    ) -> Result<Vec<Hit>> {
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let stats = match scoring {
            Scoring::Global => {
                let parts = searchers
                    .iter()
                    .map(|s| part_stats(s, query.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                Some(GlobalStats::sum(&parts, searchers.clone()))
            }
            Scoring::Local => None,
        };
        let mut hits = Vec::new();
        for (i, s) in searchers.iter().enumerate() {
            let c = TopDocs::with_limit(k).order_by_score();
            let top = match &stats {
                Some(g) => s.search_with_statistics_provider(query.as_ref(), &c, g),
                None => s.search(query.as_ref(), &c),
            }
            .map_err(terr)?;
            for (score, a) in top {
                hits.push(Hit {
                    score,
                    part: self.parts[i].no,
                    key: self.key_of(s, a)?,
                });
            }
        }
        hits.sort_by(order);
        hits.truncate(k);
        Ok(hits)
    }

    /// Every hit, up to `max` (`WHERE MATCH`); 39827 past it.
    ///
    /// # Errors
    /// As [`SearchIndex::top`]; 39827.
    pub fn all(&self, q: &SearchQuery, max: usize, max_expansions: usize) -> Result<Vec<Hit>> {
        let n = self.count(q, max_expansions)?;
        if n > max as u64 {
            return Err(SprigError::new(
                SprigCode::SearchWindowExceeded,
                // No count: it would include rows the caller may not see.
                format!("the search matched more than search.max_window ({max}) rows"),
            ));
        }
        #[allow(clippy::cast_possible_truncation)]
        self.top(q, n as usize, Scoring::Global, max_expansions)
    }

    /// Matching documents.
    ///
    /// # Errors
    /// As [`SearchIndex::top`].
    pub fn count(&self, q: &SearchQuery, max_expansions: usize) -> Result<u64> {
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        let mut n = 0u64;
        for s in &searchers {
            n += s.search(query.as_ref(), &Count).map_err(terr)? as u64;
        }
        Ok(n)
    }

    /// Facet counts of `field` over the documents matching `q`: keyword
    /// values (the top `limit`), or numbers in `boundaries` buckets.
    ///
    /// # Errors
    /// 39829 for a field that isn't a facet; Tantivy's.
    pub fn facet(
        &self,
        q: &SearchQuery,
        field: &str,
        limit: usize,
        boundaries: Option<&[f64]>,
        max_expansions: usize,
    ) -> Result<Vec<Bucket>> {
        let def = &self.schema.def;
        let want_num = boundaries.is_some();
        let f = def
            .fields
            .iter()
            .filter(|f| f.name == field && f.fast())
            .find(|f| {
                if want_num {
                    matches!(f.kind, crate::def::FieldKind::Number)
                } else {
                    matches!(f.kind, crate::def::FieldKind::Keyword)
                }
            })
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::SearchFieldType,
                    format!("'{field}' is not a facet field of this index"),
                )
            })?;
        let name = format!("f{}", f.field_no);
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut buckets: Vec<u64> = vec![0; boundaries.map_or(0, |b| b.len().saturating_sub(1))];
        for s in &searchers {
            let docs = s.search(query.as_ref(), &DocSetCollector).map_err(terr)?;
            let mut by_seg: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for a in docs {
                by_seg.entry(a.segment_ord).or_default().push(a.doc_id);
            }
            for (seg, ids) in by_seg {
                let ff = s.segment_reader(seg).fast_fields();
                if let Some(b) = boundaries {
                    let col = ff.f64(&name).map_err(terr)?;
                    for d in ids {
                        for v in col.values_for_doc(d) {
                            if let Some(i) = b.windows(2).position(|w| v >= w[0] && v < w[1]) {
                                buckets[i] += 1;
                            }
                        }
                    }
                } else if let Some(col) = ff.str(&name).map_err(terr)? {
                    let mut out = String::new();
                    for d in ids {
                        let ords: Vec<u64> = col.term_ords(d).collect();
                        for o in ords {
                            out.clear();
                            if col.ord_to_str(o, &mut out).map_err(terr)? {
                                *counts.entry(out.clone()).or_default() += 1;
                            }
                        }
                    }
                }
            }
        }
        if let Some(b) = boundaries {
            return Ok(b
                .iter()
                .zip(buckets)
                .map(|(lo, count)| Bucket {
                    value: serde_json::json!(lo),
                    count,
                })
                .collect());
        }
        let mut v: Vec<(String, u64)> = counts.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(limit);
        Ok(v.into_iter()
            .map(|(value, count)| Bucket {
                value: serde_json::Value::String(value),
                count,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
