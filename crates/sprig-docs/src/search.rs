//! In-process search over a pack's chunks: BM25 keyword ranking, exact
//! cosine ranking over the embeddings, and reciprocal-rank fusion of the
//! two.
//!
//! Hybrid search, as the docs search runs it:
//!
//! ```text
//! bm25  := Bm25::search(query, 40)
//! vec   := cosine_top(embed_query(query), 40)
//! hits  := cap_per_page(rrf([bm25, vec], 60), 2, limit)
//! ```

use std::collections::HashMap;

use frostem::{Algorithm, Stemmer};

use crate::chunk::query_terms;
use crate::pack::Chunk;

/// The rank constant of reciprocal-rank fusion.
pub const RRF_K: f64 = 60.0;
/// How much more an exact identifier match counts than a word match.
pub const IDENTIFIER_BOOST: f64 = 3.0;
/// Hits taken from each list before fusion.
pub const CANDIDATES: usize = 40;
/// The most hits fusion keeps from one page.
pub const MAX_PER_PAGE: usize = 2;

/// BM25's term-frequency saturation.
const K1: f64 = 1.2;
/// BM25's length normalisation.
const B: f64 = 0.75;

/// Common English words that carry no meaning in a search.
const STOP_WORDS: &[&str] = &[
    "a", "about", "an", "and", "are", "as", "at", "be", "but", "by", "can", "do", "does", "for",
    "from", "how", "i", "if", "in", "into", "is", "it", "its", "me", "my", "no", "not", "of", "on",
    "or", "s", "should", "such", "that", "the", "their", "then", "there", "these", "they", "this",
    "to", "was", "we", "what", "when", "where", "which", "who", "why", "will", "with", "you",
    "your",
];

/// Lower-cases `text`, splits it into Unicode words (letters and digits),
/// drops [`STOP_WORDS`] and stems what remains with the Snowball English
/// stemmer: the same steps as the full-text `english` analyzer.
#[must_use]
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = Stemmer::new(Algorithm::English);
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .filter(|w| !STOP_WORDS.contains(&w.as_str()))
        .map(|w| stemmer.stem(&w).into_owned())
        .collect()
}

/// An identifier as an index key, apart from words.
fn id_key(term: &str) -> String {
    format!("\u{1}{}", term.to_lowercase())
}

/// A BM25 index over chunks: each chunk's heading path and text as words,
/// plus its identifier terms as exact keys.
#[derive(Debug, Clone, Default)]
pub struct Bm25 {
    /// Per document: its length in words and its term frequencies.
    docs: Vec<(usize, HashMap<String, u32>)>,
    /// Documents containing each key.
    df: HashMap<String, u32>,
    avg_len: f64,
}

impl Bm25 {
    /// Indexes documents given as (searchable text, identifier terms).
    pub fn new<'a>(docs: impl IntoIterator<Item = (String, &'a [String])>) -> Self {
        let mut index = Self::default();
        let mut total = 0usize;
        for (text, terms) in docs {
            let words = analyze(&text);
            let mut tf: HashMap<String, u32> = HashMap::new();
            for w in &words {
                *tf.entry(w.clone()).or_default() += 1;
            }
            for t in terms {
                *tf.entry(id_key(t)).or_default() += 1;
            }
            for k in tf.keys() {
                *index.df.entry(k.clone()).or_default() += 1;
            }
            total += words.len();
            index.docs.push((words.len(), tf));
        }
        #[allow(clippy::cast_precision_loss)]
        if !index.docs.is_empty() {
            index.avg_len = total as f64 / index.docs.len() as f64;
        }
        index
    }

    /// Indexes a pack's chunks (heading path and text).
    #[must_use]
    pub fn for_chunks(chunks: &[Chunk]) -> Self {
        Self::new(chunks.iter().map(|c| {
            (
                format!("{}\n{}", c.heading_path, c.text),
                c.terms.as_slice(),
            )
        }))
    }

    /// The best `limit` documents for `query`, as (index, score), best
    /// first; ties go to the lower index. Words in the query count once
    /// each; identifiers ([`query_terms`]) count [`IDENTIFIER_BOOST`] times.
    #[must_use]
    pub fn search(&self, query: &str, limit: usize) -> Vec<(usize, f64)> {
        let mut keys: Vec<(String, f64)> = Vec::new();
        for w in analyze(query) {
            if !keys.iter().any(|(k, _)| *k == w) {
                keys.push((w, 1.0));
            }
        }
        for t in query_terms(query) {
            let k = id_key(&t);
            if !keys.iter().any(|(x, _)| *x == k) {
                keys.push((k, IDENTIFIER_BOOST));
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let n = self.docs.len() as f64;
        let mut scores = vec![0f64; self.docs.len()];
        for (k, weight) in &keys {
            let Some(&df) = self.df.get(k) else { continue };
            let df = f64::from(df);
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for (i, (len, tf)) in self.docs.iter().enumerate() {
                if let Some(&f) = tf.get(k) {
                    let f = f64::from(f);
                    #[allow(clippy::cast_precision_loss)]
                    let norm = K1 * (1.0 - B + B * (*len as f64) / self.avg_len.max(1.0));
                    scores[i] += weight * idf * f * (K1 + 1.0) / (f + norm);
                }
            }
        }
        top(
            scores.into_iter().enumerate().filter(|(_, s)| *s > 0.0),
            limit,
        )
    }
}

/// The `limit` vectors most similar to `query` by cosine, as (index,
/// similarity), best first. Every vector must be unit length, so cosine is
/// the dot product.
#[must_use]
pub fn cosine_top(query: &[f32], vectors: &[Vec<f32>], limit: usize) -> Vec<(usize, f64)> {
    top(
        vectors.iter().enumerate().map(|(i, v)| {
            let dot: f32 = v.iter().zip(query).map(|(a, b)| a * b).sum();
            (i, f64::from(dot))
        }),
        limit,
    )
}

fn top(scores: impl Iterator<Item = (usize, f64)>, limit: usize) -> Vec<(usize, f64)> {
    let mut v: Vec<(usize, f64)> = scores.collect();
    v.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    v.truncate(limit);
    v
}

/// Reciprocal-rank fusion: each document scores `Σ 1 / (k + rank)` over
/// the lists it appears in (ranks start at 1). Best first; ties go to the
/// lower index.
#[must_use]
pub fn rrf(lists: &[&[usize]], k: f64) -> Vec<(usize, f64)> {
    let weighted: Vec<(&[usize], f64)> = lists.iter().map(|l| (*l, 1.0)).collect();
    rrf_weighted(&weighted, k)
}

/// [`rrf`] with a weight per list: each document scores
/// `Σ weight / (k + rank)`.
#[must_use]
pub fn rrf_weighted(lists: &[(&[usize], f64)], k: f64) -> Vec<(usize, f64)> {
    let mut scores: HashMap<usize, f64> = HashMap::new();
    for (list, weight) in lists {
        for (rank, &doc) in list.iter().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let r = (rank + 1) as f64;
            *scores.entry(doc).or_default() += weight / (k + r);
        }
    }
    top(scores.into_iter(), usize::MAX)
}

/// The weight of the keyword list in fusion: [`IDENTIFIER_BOOST`] when the
/// query names an exact identifier (an error code, error name, SQLSTATE or
/// setting key), which keyword search matches exactly and the embedding
/// model doesn't know; otherwise 1, the same as the vector list.
#[must_use]
pub fn keyword_weight(query: &str) -> f64 {
    if query_terms(query).is_empty() {
        1.0
    } else {
        IDENTIFIER_BOOST
    }
}

/// Keeps at most `per_page` hits from each page, in order, up to `limit`.
#[must_use]
pub fn cap_per_page<'a>(
    hits: &[(usize, f64)],
    page_of: impl Fn(usize) -> &'a str,
    per_page: usize,
    limit: usize,
) -> Vec<(usize, f64)> {
    let mut seen: HashMap<&str, usize> = HashMap::new();
    let mut out = Vec::new();
    for &(doc, score) in hits {
        let n = seen.entry(page_of(doc)).or_default();
        if *n < per_page {
            *n += 1;
            out.push((doc, score));
            if out.len() == limit {
                break;
            }
        }
    }
    out
}

/// The indexes of `hits`.
#[must_use]
pub fn ids(hits: &[(usize, f64)]) -> Vec<usize> {
    hits.iter().map(|h| h.0).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzes_like_the_english_analyzer() {
        assert_eq!(
            analyze("How do I move Shards between clusters?"),
            ["move", "shard", "between", "cluster"]
        );
    }

    #[test]
    fn bm25_ranks_rare_terms_and_boosts_identifiers() {
        let none: [String; 0] = [];
        let code = ["39134".to_owned()];
        let docs = [
            (
                "shard moves copy rows between clusters".to_owned(),
                &none[..],
            ),
            (
                "the cluster has many shards and shards".to_owned(),
                &none[..],
            ),
            (
                "error 39134 means the source diverged".to_owned(),
                &code[..],
            ),
            ("unrelated text about bananas".to_owned(), &none[..]),
        ];
        let idx = Bm25::new(docs.iter().map(|(t, x)| (t.clone(), *x)));
        let hits = idx.search("moving shards", 10);
        assert_eq!(ids(&hits), [0, 1]);
        assert!(hits[0].1 > hits[1].1);
        assert!(idx.search("bananas", 10)[0].0 == 3);
        assert!(idx.search("the", 10).is_empty());
        // The identifier key adds three times a word match's weight.
        let with_id = idx.search("39134", 10);
        assert_eq!(ids(&with_id), [2]);
        let words_only = Bm25::new(docs.iter().map(|(t, _)| (t.clone(), &none[..])));
        let plain = words_only.search("39134", 10);
        assert!((with_id[0].1 / plain[0].1 - 4.0).abs() < 1e-9);
    }

    #[test]
    fn cosine_ranks_by_dot_product() {
        let v = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.6, 0.8]];
        assert_eq!(ids(&cosine_top(&[0.8, 0.6], &v, 2)), [2, 0]);
    }

    #[test]
    fn weighted_rrf_and_keyword_weight() {
        // Doc 7 is first by keywords, doc 8 first by vectors.
        let plain = rrf_weighted(&[(&[7, 8], 1.0), (&[8, 7], 1.0)], 60.0);
        assert_eq!(ids(&plain), [7, 8]);
        assert!((plain[0].1 - plain[1].1).abs() < 1e-12);
        let boosted = rrf_weighted(&[(&[7, 9], 3.0), (&[8, 9], 1.0)], 60.0);
        assert_eq!(ids(&boosted), [9, 7, 8]);
        assert!((boosted[1].1 - 3.0 / 61.0).abs() < 1e-12);
        assert!((keyword_weight("what does 39420 mean") - IDENTIFIER_BOOST).abs() < 1e-12);
        assert!((keyword_weight("set auto_index.mode to off") - IDENTIFIER_BOOST).abs() < 1e-12);
        assert!((keyword_weight("how do I shard a table") - 1.0).abs() < 1e-12);
    }

    #[test]
    fn rrf_fuses_ranks_and_caps_pages() {
        let fused = rrf(&[&[1, 2, 3], &[3, 1, 4]], 60.0);
        // 1: 1/61 + 1/62; 3: 1/63 + 1/61; 2: 1/62; 4: 1/63.
        assert_eq!(ids(&fused), [1, 3, 2, 4]);
        assert!((fused[0].1 - (1.0 / 61.0 + 1.0 / 62.0)).abs() < 1e-12);
        let pages = ["a", "a", "a", "a", "b"];
        let capped = cap_per_page(&fused, |i| pages[i], 2, 10);
        assert_eq!(ids(&capped), [1, 3, 4]);
        assert_eq!(ids(&cap_per_page(&fused, |i| pages[i], 2, 1)), [1]);
        // Ties go to the lower index.
        assert_eq!(ids(&rrf(&[&[5], &[2]], 60.0)), [2, 5]);
    }
}
