//! Cutting a page into chunks for search, and finding the exact identifiers
//! in a chunk.
//!
//! # Chunking
//!
//! A page is split at its headings (`#`, `##`, `###`; deeper headings stay
//! inside their section), and never inside a fenced code block, a table row
//! or a paragraph line:
//!
//! 1. Each section's heading path starts with the page title (or its `#`
//!    heading) and adds the `##` and `###` headings above it, for example
//!    `Automatic indexing > Validation > Replica builds`.
//! 2. A section longer than [`MAX_TOKENS`] is split between blocks
//!    (paragraphs, lists, code blocks, tables) into pieces of about
//!    [`MIN_SPLIT_TOKENS`]–[`MAX_TOKENS`] tokens. Every piece keeps the
//!    section's heading path, and pieces after the first repeat the
//!    section's first sentence. A table too long for one piece is split
//!    between rows, each part with the table's header. Code blocks are
//!    always kept whole, even when longer.
//! 3. A section under [`MERGE_BELOW_TOKENS`] tokens is merged, with its
//!    heading line, into the chunk before it when that chunk is its parent
//!    or a sibling (same parent) and the result stays within
//!    [`MAX_TOKENS`].
//!
//! Sections with no text of their own (a heading followed straight by a
//! sub-heading) make no chunk; the heading path of their children names
//! them. Lines that are only an HTML comment, such as the generated pages'
//! note, are dropped.
//!
//! # Token estimate
//!
//! [`estimate_tokens`] approximates a BERT word-piece tokenizer without
//! loading one: every punctuation mark or symbol is one token, and every run
//! of letters and digits is one token plus one more for each further six
//! characters (long identifiers split into several word pieces). On these
//! docs it lands within about 15% of the real tokenizer's count.

use std::fmt::Write as _;

use crate::page::Page;

/// The most tokens a chunk should have. The embedding model reads 512,
/// including the heading path in front of the chunk, and the estimate runs
/// a little under the model's count, so this leaves room.
pub const MAX_TOKENS: usize = 460;
/// Where a long section's pieces are cut once they are big enough.
pub const MIN_SPLIT_TOKENS: usize = 300;
/// Sections shorter than this are merged into the chunk before them.
pub const MERGE_BELOW_TOKENS: usize = 80;
/// The separator between headings in a heading path.
pub const PATH_SEP: &str = " > ";

/// A chunk of one page, before it gets an id and an embedding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageChunk {
    /// The headings above the chunk, the page title first.
    pub heading_path: Vec<String>,
    /// The chunk's Markdown.
    pub text: String,
    /// [`estimate_tokens`] of `text`.
    pub tokens: usize,
}

impl PageChunk {
    /// The heading path joined with [`PATH_SEP`].
    #[must_use]
    pub fn heading_path_str(&self) -> String {
        self.heading_path.join(PATH_SEP)
    }
}

/// The estimated number of model tokens in `text` (see the module docs).
#[must_use]
pub fn estimate_tokens(text: &str) -> usize {
    let mut n = 0;
    let mut run = 0usize;
    for c in text.chars() {
        if c.is_alphanumeric() {
            run += 1;
        } else {
            if run > 0 {
                n += 1 + (run - 1) / 6;
                run = 0;
            }
            if !c.is_whitespace() {
                n += 1;
            }
        }
    }
    if run > 0 {
        n += 1 + (run - 1) / 6;
    }
    n
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Para,
    Code,
    Table,
}

#[derive(Debug, Clone)]
struct Block {
    kind: Kind,
    lines: Vec<String>,
}

impl Block {
    fn text(&self) -> String {
        self.lines.join("\n")
    }
}

#[derive(Debug)]
struct Section {
    path: Vec<String>,
    /// The heading line as written (`## Foo`), empty for the page's start.
    heading: String,
    blocks: Vec<Block>,
}

/// A fence opener: its character and length, when `line` opens one.
fn fence(line: &str) -> Option<(char, usize)> {
    let t = line.trim_start();
    let c = t.chars().next().filter(|&c| c == '`' || c == '~')?;
    let n = t.chars().take_while(|&x| x == c).count();
    (n >= 3).then_some((c, n))
}

/// `(level, text)` when `line` is an ATX heading.
fn heading(line: &str) -> Option<(usize, String)> {
    let n = line.chars().take_while(|&c| c == '#').count();
    if !(1..=6).contains(&n) {
        return None;
    }
    let rest = &line[n..];
    if !rest.is_empty() && !rest.starts_with(' ') {
        return None;
    }
    Some((n, rest.trim().trim_end_matches('#').trim().to_owned()))
}

fn is_comment_line(line: &str) -> bool {
    let t = line.trim();
    t.starts_with("<!--") && t.ends_with("-->")
}

/// Splits a page body into sections of blocks.
fn sections(title: &str, body: &str) -> Vec<Section> {
    let mut out = vec![Section {
        path: vec![title.to_owned()],
        heading: String::new(),
        blocks: Vec::new(),
    }];
    let mut cur: Option<Block> = None;
    let mut open_fence: Option<(char, usize)> = None;
    let flush = |cur: &mut Option<Block>, out: &mut Vec<Section>| {
        if let (Some(b), Some(s)) = (cur.take(), out.last_mut()) {
            s.blocks.push(b);
        }
    };
    for line in body.lines() {
        if let Some((c, n)) = open_fence {
            if let Some(b) = cur.as_mut() {
                b.lines.push(line.to_owned());
            }
            let t = line.trim();
            if t.chars().take_while(|&x| x == c).count() >= n && t.chars().all(|x| x == c) {
                open_fence = None;
                flush(&mut cur, &mut out);
            }
            continue;
        }
        if let Some(f) = fence(line) {
            flush(&mut cur, &mut out);
            open_fence = Some(f);
            cur = Some(Block {
                kind: Kind::Code,
                lines: vec![line.to_owned()],
            });
            continue;
        }
        if is_comment_line(line) {
            continue;
        }
        if let Some((level, text)) = heading(line).filter(|(l, _)| *l <= 3) {
            flush(&mut cur, &mut out);
            let mut path = out.last().map(|s| s.path.clone()).unwrap_or_default();
            if level == 1 {
                path = vec![text];
            } else {
                path.truncate(level - 1);
                if path.is_empty() {
                    path.push(title.to_owned());
                }
                path.push(text);
            }
            out.push(Section {
                path,
                heading: line.trim_end().to_owned(),
                blocks: Vec::new(),
            });
            continue;
        }
        if line.trim().is_empty() {
            flush(&mut cur, &mut out);
            continue;
        }
        let kind = if line.trim_start().starts_with('|') {
            Kind::Table
        } else {
            Kind::Para
        };
        match cur.as_mut() {
            Some(b) if b.kind == kind => b.lines.push(line.to_owned()),
            _ => {
                flush(&mut cur, &mut out);
                cur = Some(Block {
                    kind,
                    lines: vec![line.to_owned()],
                });
            }
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// Splits blocks too long for one chunk where that is allowed: tables
/// between rows (each part keeps the header and delimiter rows), paragraphs
/// and lists between lines. Code blocks stay whole.
fn expand(blocks: &[Block]) -> Vec<Block> {
    let mut out = Vec::new();
    for b in blocks {
        if b.kind == Kind::Code || estimate_tokens(&b.text()) <= MAX_TOKENS {
            out.push(b.clone());
            continue;
        }
        let (head, rows) = if b.kind == Kind::Table && b.lines.len() > 2 {
            b.lines.split_at(2)
        } else {
            b.lines.split_at(0)
        };
        let head_tokens = estimate_tokens(&head.join("\n"));
        let mut part: Vec<String> = head.to_vec();
        let mut tokens = head_tokens;
        for row in rows {
            let t = estimate_tokens(row) + 1;
            if part.len() > head.len() && tokens + t > MAX_TOKENS {
                out.push(Block {
                    kind: b.kind,
                    lines: std::mem::replace(&mut part, head.to_vec()),
                });
                tokens = head_tokens;
            }
            part.push(row.clone());
            tokens += t;
        }
        if part.len() > head.len() {
            out.push(Block {
                kind: b.kind,
                lines: part,
            });
        }
    }
    out
}

fn join(blocks: &[Block]) -> String {
    blocks
        .iter()
        .map(Block::text)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The first sentence of the section's opening paragraph, if it is short.
fn first_sentence(blocks: &[Block]) -> Option<String> {
    let b = blocks.first().filter(|b| b.kind == Kind::Para)?;
    let text = b.lines.join(" ");
    let end = text
        .match_indices(". ")
        .map(|(i, _)| i + 1)
        .next()
        .unwrap_or(text.len());
    let s = text[..end].trim();
    (estimate_tokens(s) <= 60 && !s.is_empty()).then(|| s.to_owned())
}

/// Splits a long section's blocks into pieces (see the module docs).
fn split(blocks: &[Block]) -> Vec<Vec<Block>> {
    let total = estimate_tokens(&join(blocks));
    let pieces = total.div_ceil(MAX_TOKENS).max(1);
    let target = (total / pieces).max(MIN_SPLIT_TOKENS);
    let mut out: Vec<Vec<Block>> = Vec::new();
    let mut cur: Vec<Block> = Vec::new();
    let mut cur_tokens = 0;
    for b in blocks {
        let t = estimate_tokens(&b.text());
        if !cur.is_empty() && (cur_tokens + t > MAX_TOKENS || cur_tokens >= target) {
            out.push(std::mem::take(&mut cur));
            cur_tokens = 0;
        }
        cur.push(b.clone());
        cur_tokens += t;
    }
    if !cur.is_empty() {
        // A small tail joins the piece before it when they fit together.
        match out.last_mut() {
            Some(prev)
                if cur_tokens < MERGE_BELOW_TOKENS
                    && estimate_tokens(&join(prev)) + cur_tokens <= MAX_TOKENS =>
            {
                prev.extend(cur);
            }
            _ => out.push(cur),
        }
    }
    out
}

/// Cuts `page` into chunks, in page order (see the module docs).
#[must_use]
pub fn chunk_page(page: &Page) -> Vec<PageChunk> {
    let mut out: Vec<PageChunk> = Vec::new();
    for s in sections(&page.title, &page.body) {
        if s.blocks.is_empty() {
            continue;
        }
        let blocks = expand(&s.blocks);
        let text = join(&blocks);
        let tokens = estimate_tokens(&text);
        let parent = &s.path[..s.path.len() - 1];
        if tokens < MERGE_BELOW_TOKENS
            && !s.heading.is_empty()
            && let Some(prev) = out.last_mut()
        {
            let related = prev.heading_path.as_slice() == parent
                || (prev.heading_path.len() == s.path.len()
                    && prev.heading_path[..prev.heading_path.len() - 1] == *parent);
            let merged = format!("{}\n\n{}\n\n{}", prev.text, s.heading, text);
            let merged_tokens = estimate_tokens(&merged);
            if related && merged_tokens <= MAX_TOKENS {
                prev.text = merged;
                prev.tokens = merged_tokens;
                continue;
            }
        }
        if tokens <= MAX_TOKENS {
            out.push(PageChunk {
                heading_path: s.path.clone(),
                text,
                tokens,
            });
            continue;
        }
        let lead = first_sentence(&blocks);
        for (i, piece) in split(&blocks).into_iter().enumerate() {
            let mut text = String::new();
            if i > 0
                && let Some(lead) = &lead
                && !piece[0].text().starts_with(lead.as_str())
            {
                let _ = write!(text, "{lead}\n\n");
            }
            text.push_str(&join(&piece));
            out.push(PageChunk {
                heading_path: s.path.clone(),
                tokens: estimate_tokens(&text),
                text,
            });
        }
    }
    out
}

// ---- identifiers ----

/// Last segments of dotted names that are file names, not settings.
const FILE_EXTENSIONS: [&str; 22] = [
    "md", "toml", "rs", "sh", "yaml", "yml", "json", "py", "js", "ts", "mjs", "html", "pack",
    "txt", "sql", "pem", "crt", "key", "conf", "cnf", "log", "csv",
];

/// SQLSTATE classes (the first two characters) that appear as all-digit
/// codes.
const SQLSTATE_CLASSES: [&str; 18] = [
    "01", "02", "07", "08", "21", "22", "23", "24", "25", "26", "27", "28", "34", "40", "42", "44",
    "45", "53",
];

fn is_error_code(w: &str) -> bool {
    w.len() == 5 && w.starts_with("39") && w.bytes().all(|b| b.is_ascii_digit())
}

fn is_sqlstate(w: &str) -> bool {
    if w.len() != 5 || is_error_code(w) {
        return false;
    }
    let b = w.as_bytes();
    if !b
        .iter()
        .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    {
        return false;
    }
    let letters = b.iter().filter(|c| c.is_ascii_uppercase()).count();
    if letters == 0 {
        return SQLSTATE_CLASSES.contains(&&w[..2]);
    }
    // HY000, 08S01, 42S02, 0A000, 3D000, XA100: a digit after the class.
    let digits = b.iter().filter(|c| c.is_ascii_digit()).count();
    digits >= 3 && (w.starts_with("HY") || w.starts_with("XA") || b[0].is_ascii_digit())
}

fn is_error_name(w: &str) -> bool {
    w.len() > 9
        && w.starts_with("ER_SPRIG_")
        && w.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// A MongoDB `codeName` such as `SprigShardMoved` (not the product's name).
fn is_code_name(w: &str) -> bool {
    w.len() > 6
        && w != "SprigDB"
        && w.starts_with("Sprig")
        && w[5..].starts_with(|c: char| c.is_ascii_uppercase())
        && w.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A setting key or other dotted lower-case name: `auto_index.mode`,
/// `listener.<name>.bind`. With `strict`, a segment must contain an
/// underscore unless the caller found the name in backticks.
fn is_dotted(w: &str, strict: bool) -> bool {
    let segs: Vec<&str> = w.split('.').collect();
    if segs.len() < 2 || !w.starts_with(|c: char| c.is_ascii_lowercase()) {
        return false;
    }
    let seg_ok = |s: &&str| {
        s.len() >= 2
            && (s
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
                || (s.starts_with('<')
                    && s.ends_with('>')
                    && s[1..s.len() - 1]
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_')))
    };
    if !segs.iter().all(seg_ok) {
        return false;
    }
    if segs.last().is_some_and(|l| FILE_EXTENSIONS.contains(l)) {
        return false;
    }
    !strict || w.contains('_')
}

/// Words for identifier matching: runs of letters, digits, `_`, `.`, `<`
/// and `>`, with trailing dots dropped.
fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '<' | '>')))
        .map(|w| w.trim_matches(|c| c == '.' || c == '<' || c == '>'))
        .filter(|w| !w.is_empty())
}

/// Like [`words`], but `<` and `>` kept on segments (`listener.<name>.bind`).
fn dotted_words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '<' | '>')))
        .map(|w| w.trim_end_matches('.').trim_start_matches('.'))
        .filter(|w| !w.is_empty())
}

fn plain_identifier(w: &str) -> bool {
    is_error_code(w) || is_error_name(w) || is_code_name(w) || is_sqlstate(w)
}

/// The exact identifiers in a chunk's text, sorted and without duplicates:
/// error codes (`39412`), error names (`ER_SPRIG_SHARD_MOVED`) and MongoDB
/// code names (`SprigShardMoved`), SQLSTATEs (`HY000`, `08S01`, `40001`),
/// and setting keys (`auto_index.mode`). A dotted name counts when it is
/// written in backticks, or anywhere when it has an underscore.
#[must_use]
pub fn terms(text: &str) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();
    for w in words(text) {
        for part in w.split('.') {
            if plain_identifier(part) {
                out.insert(part.to_owned());
            }
        }
    }
    for w in dotted_words(text) {
        if is_dotted(w, true) {
            out.insert(w.to_owned());
        }
    }
    for (i, span) in text.split('`').enumerate() {
        if i % 2 == 1 {
            for w in dotted_words(span) {
                if is_dotted(w, false) {
                    out.insert(w.to_owned());
                }
            }
        }
    }
    out.into_iter().collect()
}

/// The identifiers in a search query: the same kinds as [`terms`], but
/// dotted names count without backticks, and error names and SQLSTATEs in
/// any case (`er_sprig_shard_moved`, `hy000`).
#[must_use]
pub fn query_terms(query: &str) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();
    for w in dotted_words(query) {
        if is_dotted(w, false) {
            out.insert(w.to_owned());
            continue;
        }
        let w = w.trim_matches(|c| c == '<' || c == '>');
        let upper = w.to_ascii_uppercase();
        if is_code_name(w) || is_error_code(w) {
            out.insert(w.to_owned());
        } else if is_error_name(&upper) || is_sqlstate(&upper) {
            out.insert(upper);
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_tokens_like_word_pieces() {
        assert_eq!(estimate_tokens(""), 0);
        assert_eq!(estimate_tokens("shard moves"), 2);
        // auto _ index . mode
        assert_eq!(estimate_tokens("auto_index.mode"), 5);
        // a 13-letter word counts as 3
        assert_eq!(estimate_tokens("authoritative"), 3);
    }

    #[test]
    fn finds_identifiers() {
        let t = terms(
            "Error 39134 (`ER_SPRIG_MOVE_SOURCE_DIVERGED`, SprigMoveSourceDiverged), SQLSTATE \
             HY000 or 08S01 or 40001. Set `auto_index.mode` or `advisor.mode`, and \
             mover.copy_max_rows_per_sec; see `errors.md`, e.g. 50000 rows. SprigDB.",
        );
        assert_eq!(
            t,
            [
                "08S01",
                "39134",
                "40001",
                "ER_SPRIG_MOVE_SOURCE_DIVERGED",
                "HY000",
                "SprigMoveSourceDiverged",
                "advisor.mode",
                "auto_index.mode",
                "mover.copy_max_rows_per_sec",
            ]
        );
        assert_eq!(terms("`listener.<name>.bind`"), ["listener.<name>.bind"]);
        assert_eq!(
            query_terms("what does er_sprig_shard_moved or 39420 mean, and advisor.mode?"),
            ["39420", "ER_SPRIG_SHARD_MOVED", "advisor.mode"]
        );
    }
}
