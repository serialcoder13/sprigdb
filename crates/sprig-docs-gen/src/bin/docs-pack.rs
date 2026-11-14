//! `docs-pack`: builds the docs pack from `docs/` and the embedding model,
//! and measures how well it answers a golden set of questions.
//!
//! ```text
//! docs-pack --model <dir> [--version <v>] [--out <file>] [--root <repo>] [--check-repro]
//! docs-pack eval --model <dir> [--pack <file>] [--golden <file>] [--version <v>]
//!                [--root <repo>] [--verbose]
//! docs-pack stats [--root <repo>]
//! ```
//!
//! Build it optimised; the model is slow without optimisation:
//! `cargo run --release -p sprig-docs-gen --no-default-features --bin docs-pack -- ...`.
//!
//! - **Build** reads every page under `docs/` (except `pdf/` and `tests/`)
//!   in path order, cuts them into chunks, embeds each chunk with the model
//!   and writes the pack (default `target/docs/sprig-docs-<version>.pack`).
//!   It prints the chunk count, the size and the pack's SHA-256.
//!   `--check-repro` builds twice from scratch and fails unless both builds
//!   give the same bytes. The version defaults to the workspace's.
//! - **Eval** loads a pack, runs each question of the golden set
//!   (default `docs/tests/retrieval-golden.toml`) through vector-only,
//!   BM25-only and hybrid search, and reports recall@5 for each: the share
//!   of questions with an answering chunk in the top five hits. Hybrid is
//!   reciprocal-rank fusion (k = 60, at most two hits per page), with the
//!   keyword list's vote weighted by the identifier boost when the query
//!   names an exact identifier; plain fusion is reported beside it. Exits 1
//!   when hybrid recall@5 is under 0.9.
//! - **Stats** chunks the docs without the model and prints the chunk count
//!   and sizes, the largest chunks first.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use serde::Deserialize;
use sprig_docs::pack::{Chunk, Pack, chunk_pages, hex};
use sprig_docs::page::read_pages;
use sprig_docs::search::{
    Bm25, CANDIDATES, MAX_PER_PAGE, RRF_K, cap_per_page, cosine_top, ids, keyword_weight, rrf,
    rrf_weighted,
};
use sprig_embed::Embedder;

/// Hits a question may look at.
const TOP: usize = 5;

struct Args {
    rest: Vec<String>,
}

impl Args {
    fn flag(&self, name: &str) -> Option<&str> {
        self.rest
            .iter()
            .position(|a| a == name)
            .and_then(|i| self.rest.get(i + 1))
            .map(String::as_str)
    }
    fn has(&self, name: &str) -> bool {
        self.rest.iter().any(|a| a == name)
    }
}

fn main() -> ExitCode {
    let mut rest: Vec<String> = std::env::args().skip(1).collect();
    let mode = match rest.first().map(String::as_str) {
        Some(m @ ("eval" | "stats")) => {
            let m = m.to_owned();
            rest.remove(0);
            m
        }
        _ => "build".to_owned(),
    };
    let args = Args { rest };
    if args.has("--help") || args.has("-h") {
        println!(
            "docs-pack --model <dir> [--version <v>] [--out <file>] [--root <repo>] [--check-repro]\n\
             docs-pack eval --model <dir> [--pack <file>] [--golden <file>] [--version <v>] [--root <repo>] [--verbose]\n\
             docs-pack stats [--root <repo>]"
        );
        return ExitCode::SUCCESS;
    }
    let result = match mode.as_str() {
        "eval" => run_eval(&args),
        "stats" => run_stats(&args),
        _ => run_build(&args),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("docs-pack: {e}");
            ExitCode::from(2)
        }
    }
}

fn root(args: &Args) -> PathBuf {
    args.flag("--root").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
        PathBuf::from,
    )
}

fn version(args: &Args) -> String {
    args.flag("--version")
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_owned()
}

fn default_pack(root: &Path, version: &str) -> PathBuf {
    root.join(format!("target/docs/sprig-docs-{version}.pack"))
}

fn load_model(args: &Args) -> Result<Embedder, String> {
    let dir = args
        .flag("--model")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("SPRIG_EMBED_MODEL_DIR").map(PathBuf::from))
        .ok_or("--model <dir> is required (scripts/fetch-embed-model.sh fetches it)")?;
    Embedder::load(&dir).map_err(|e| e.to_string())
}

/// Builds the pack's bytes from the docs and the model.
fn build(root: &Path, version: &str, model: &Embedder) -> Result<(Vec<u8>, usize), String> {
    let pages = read_pages(&root.join("docs"))?;
    let mut chunks = chunk_pages(version, &pages);
    let inputs: Vec<String> = chunks.iter().map(Chunk::embedding_input).collect();
    let refs: Vec<&str> = inputs.iter().map(String::as_str).collect();
    let vectors = model.embed_passages(&refs).map_err(|e| e.to_string())?;
    for (c, v) in chunks.iter_mut().zip(&vectors) {
        c.set_embedding(v);
    }
    let pack = Pack {
        version: version.to_owned(),
        model_id: model.model_id().to_owned(),
        model_sha256: model.model_sha256(),
        chunks,
    };
    Ok((pack.to_bytes().map_err(|e| e.to_string())?, pages.len()))
}

fn run_build(args: &Args) -> Result<ExitCode, String> {
    let root = root(args);
    let version = version(args);
    let out = args
        .flag("--out")
        .map_or_else(|| default_pack(&root, &version), PathBuf::from);
    let model = load_model(args)?;

    let start = Instant::now();
    let (bytes, pages) = build(&root, &version, &model)?;
    let took = start.elapsed();
    let pack = Pack::from_bytes(&bytes).map_err(|e| e.to_string())?;
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    std::fs::write(&out, &bytes).map_err(|e| format!("{}: {e}", out.display()))?;
    let sha = hex(&bytes[bytes.len() - 32..]);
    println!("pack     {}", out.display());
    println!("version  {version}");
    println!("model    {} {}", pack.model_id, hex(&pack.model_sha256));
    println!("pages    {pages}");
    println!("chunks   {}", pack.chunks.len());
    println!("bytes    {}", bytes.len());
    println!("sha256   {sha}");
    println!("built in {:.1}s", took.as_secs_f64());
    token_report(&pack, &model)?;

    if args.has("--check-repro") {
        let again = build(&root, &version, &model)?.0;
        if again == bytes {
            println!(
                "repro    ok: a second build gave the same {} bytes",
                bytes.len()
            );
        } else {
            eprintln!(
                "repro    FAILED: the second build differs (sha256 {})",
                hex(&again[again.len().saturating_sub(32)..])
            );
            return Ok(ExitCode::from(1));
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn run_stats(args: &Args) -> Result<ExitCode, String> {
    let pages = read_pages(&root(args).join("docs"))?;
    let chunks = chunk_pages(&version(args), &pages);
    let mut tokens: Vec<u16> = chunks.iter().map(|c| c.tokens).collect();
    tokens.sort_unstable();
    let pct = |p: usize| tokens.get(tokens.len() * p / 100).copied().unwrap_or(0);
    println!(
        "pages {}, chunks {}, estimated tokens: median {}, p10 {}, p90 {}, max {}",
        pages.len(),
        chunks.len(),
        pct(50),
        pct(10),
        pct(90),
        tokens.last().copied().unwrap_or(0)
    );
    let mut largest: Vec<&Chunk> = chunks.iter().collect();
    largest.sort_by_key(|c| std::cmp::Reverse(c.tokens));
    for c in largest.iter().take(10) {
        println!("{:>6} {} :: {}", c.tokens, c.page, c.heading_path);
    }
    Ok(ExitCode::SUCCESS)
}

/// How the chunker's token estimate compares with the model's tokenizer.
fn token_report(pack: &Pack, model: &Embedder) -> Result<(), String> {
    let mut real = Vec::with_capacity(pack.chunks.len());
    let mut over = 0;
    for c in &pack.chunks {
        let n = model
            .count_tokens(&c.embedding_input())
            .map_err(|e| e.to_string())?;
        if n > sprig_embed::MAX_TOKENS {
            over += 1;
        }
        real.push(n);
    }
    let est: usize = pack.chunks.iter().map(|c| usize::from(c.tokens)).sum();
    let total: usize = real.iter().sum();
    real.sort_unstable();
    let pct = |p: usize| real.get(real.len() * p / 100).copied().unwrap_or(0);
    #[allow(clippy::cast_precision_loss)]
    let ratio = est as f64 / total.max(1) as f64;
    println!(
        "tokens   model: median {}, p90 {}, max {}; estimate/model {ratio:.2}; {over} chunks over {} model tokens (embedded truncated)",
        pct(50),
        pct(90),
        real.last().copied().unwrap_or(0),
        sprig_embed::MAX_TOKENS
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Golden {
    question: Vec<Question>,
}

#[derive(Debug, Deserialize)]
struct Question {
    /// The question, as a user would type it.
    q: String,
    /// The page that answers it, under `docs/`.
    page: String,
    /// Part of the answering chunk's heading path, when it matters.
    heading: Option<String>,
    /// Other pages that answer it as well.
    #[serde(default)]
    also: Vec<String>,
    /// Text the answering chunk must contain, such as the error code asked
    /// about, on whichever page.
    contains: Option<String>,
    /// The kind of question, for the report.
    #[serde(default)]
    kind: String,
}

impl Question {
    fn answered_by(&self, c: &Chunk) -> bool {
        let page = c.page == self.page || self.also.contains(&c.page);
        let text = self
            .contains
            .as_ref()
            .is_none_or(|t| c.text.contains(t.as_str()));
        page && text
            && self
                .heading
                .as_ref()
                .is_none_or(|h| c.page != self.page || c.heading_path.contains(h.as_str()))
    }
}

fn read_golden(path: &Path) -> Result<Golden, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// A pack's chunks, ready to search three ways.
struct Index<'a> {
    chunks: &'a [Chunk],
    bm25: Bm25,
    vectors: Vec<Vec<f32>>,
}

impl<'a> Index<'a> {
    fn new(chunks: &'a [Chunk]) -> Self {
        Self {
            chunks,
            bm25: Bm25::for_chunks(chunks),
            vectors: chunks.iter().map(Chunk::embedding_f32).collect(),
        }
    }

    /// The top hits for each of [`MODES`], each with at most two hits
    /// per page.
    fn rank(&self, query: &str, query_vector: &[f32], top: usize) -> [Vec<(usize, f64)>; 4] {
        let page_of = |i: usize| self.chunks[i].page.as_str();
        let by_vector = cosine_top(query_vector, &self.vectors, CANDIDATES);
        let by_words = self.bm25.search(query, CANDIDATES);
        let (words, vector) = (ids(&by_words), ids(&by_vector));
        let fused = rrf_weighted(&[(&words, keyword_weight(query)), (&vector, 1.0)], RRF_K);
        let plain = rrf(&[&words, &vector], RRF_K);
        [by_vector, by_words, fused, plain].map(|l| cap_per_page(&l, page_of, MAX_PER_PAGE, top))
    }
}

/// The search modes eval compares: vector only, BM25 only, hybrid (the
/// keyword list weighted up for identifier queries), and hybrid with plain
/// reciprocal-rank fusion.
const MODES: [&str; 4] = ["vector", "bm25", "hybrid", "plain-rrf"];

/// The answer to one golden question, printed when it was missed or with
/// `--verbose`.
fn report_question(q: &Question, index: &Index<'_>, qv: &[f32], found: [bool; 4]) {
    let chunks = index.chunks;
    println!(
        "{} {}  [vector {}, bm25 {}, plain-rrf {}]  want {}{}",
        if found[2] { "ok  " } else { "MISS" },
        q.q,
        mark(found[0]),
        mark(found[1]),
        mark(found[3]),
        q.page,
        q.heading
            .as_ref()
            .map(|h| format!(" > {h}"))
            .unwrap_or_default()
    );
    // Where the first answering chunk ranks in each list (after the cap).
    let deep = index.rank(&q.q, qv, CANDIDATES);
    let pos = deep.each_ref().map(|m| {
        m.iter()
            .position(|&(i, _)| q.answered_by(&chunks[i]))
            .map_or_else(|| "-".to_owned(), |p| (p + 1).to_string())
    });
    println!(
        "       answer ranks: vector {}, bm25 {}, hybrid {}, plain-rrf {}",
        pos[0], pos[1], pos[2], pos[3]
    );
    for &(i, s) in deep[2].iter().take(TOP) {
        println!(
            "       {s:.4} {} :: {}",
            chunks[i].page, chunks[i].heading_path
        );
    }
}

fn run_eval(args: &Args) -> Result<ExitCode, String> {
    let root = root(args);
    let version = version(args);
    let pack_path = args
        .flag("--pack")
        .map_or_else(|| default_pack(&root, &version), PathBuf::from);
    let golden = read_golden(&args.flag("--golden").map_or_else(
        || root.join("docs/tests/retrieval-golden.toml"),
        PathBuf::from,
    ))?;
    let verbose = args.has("--verbose");

    let bytes = std::fs::read(&pack_path).map_err(|e| format!("{}: {e}", pack_path.display()))?;
    let pack = Pack::from_bytes(&bytes).map_err(|e| format!("{}: {e}", pack_path.display()))?;
    let load = Instant::now();
    let embedder = load_model(args)?;
    let load_time = load.elapsed();
    if embedder.model_sha256() != pack.model_sha256 {
        return Err("the model differs from the one the pack was built with".into());
    }
    let chunks = &pack.chunks;
    let index = Index::new(chunks);

    // Per kind of question: hits in each mode, then the question count.
    let mut by_kind: BTreeMap<String, [usize; 5]> = BTreeMap::new();
    let mut embed_times: Vec<Duration> = Vec::new();
    for q in &golden.question {
        let t = Instant::now();
        let qv = embedder.embed_query(&q.q).map_err(|e| e.to_string())?;
        embed_times.push(t.elapsed());
        let found = index
            .rank(&q.q, &qv, TOP)
            .map(|m| m.iter().any(|&(i, _)| q.answered_by(&chunks[i])));
        let k = by_kind.entry(q.kind.clone()).or_default();
        k[4] += 1;
        for (m, f) in found.iter().enumerate() {
            k[m] += usize::from(*f);
        }
        if verbose || !found[2] {
            report_question(q, &index, &qv, found);
        }
    }
    let n = golden.question.len();
    let hits: [usize; 4] = std::array::from_fn(|m| by_kind.values().map(|k| k[m]).sum::<usize>());
    #[allow(clippy::cast_precision_loss)]
    let r = |h: usize| h as f64 / n.max(1) as f64;
    println!();
    println!("questions {n}, chunks {}", chunks.len());
    let line = |hits: &[usize], of: usize| {
        MODES
            .iter()
            .zip(hits)
            .map(|(m, h)| format!("{m} {h}/{of}"))
            .collect::<Vec<_>>()
            .join("  ")
    };
    println!(
        "recall@{TOP}  {}",
        MODES
            .iter()
            .zip(hits)
            .map(|(m, h)| format!("{m} {:.3}", r(h)))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for (kind, k) in &by_kind {
        println!("  {kind:<16} {}", line(&k[..4], k[4]));
    }
    embed_times.sort_unstable();
    let ms = |d: Option<&Duration>| d.map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    println!(
        "query embedding: model load {:.0} ms; per query median {:.1} ms, p90 {:.1} ms, max {:.1} ms",
        ms(Some(&load_time)),
        ms(embed_times.get(embed_times.len() / 2)),
        ms(embed_times.get(embed_times.len() * 9 / 10)),
        ms(embed_times.last())
    );
    Ok(if r(hits[2]) >= 0.9 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn mark(b: bool) -> &'static str {
    if b { "hit" } else { "miss" }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The golden set parses, has about fifty questions, and names only
    /// pages and headings that exist.
    #[test]
    fn golden_set_names_real_pages() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let golden = read_golden(&root.join("docs/tests/retrieval-golden.toml")).expect("golden");
        assert!(golden.question.len() >= 45, "{}", golden.question.len());
        let pages = read_pages(&root.join("docs")).expect("pages");
        let chunks = chunk_pages("test", &pages);
        for q in &golden.question {
            for p in std::iter::once(&q.page).chain(&q.also) {
                assert!(pages.iter().any(|x| x.path == *p), "{}: no page {p}", q.q);
            }
            // Some chunk answers it, so a miss is the search's fault.
            assert!(chunks.iter().any(|c| q.answered_by(c)), "{}: no chunk", q.q);
        }
    }
}
