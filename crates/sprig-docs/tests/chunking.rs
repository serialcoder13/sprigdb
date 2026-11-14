//! Chunking golden test: `fixtures/chunking.md` gives exactly the chunks in
//! `fixtures/chunking.expected`. After a deliberate change to the chunker,
//! rewrite the expected file with `SPRIG_DOCS_REGEN=1 cargo test -p
//! sprig-docs --test chunking` and review the diff.

use std::fmt::Write as _;
use std::path::PathBuf;

use sprig_docs::chunk::MAX_TOKENS;
use sprig_docs::pack::{chunk_pages, hex};
use sprig_docs::page::parse_page;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn render() -> String {
    let text = std::fs::read_to_string(fixtures().join("chunking.md")).expect("fixture");
    let page = parse_page("fixtures/chunking.md", &text).expect("page");
    let mut out = String::new();
    for c in chunk_pages("0.0.0-test", &[page]) {
        writeln!(
            out,
            "=== {} | {} | tokens {} | audience {} | terms [{}]\n{}\n",
            hex(&c.chunk_id),
            c.heading_path,
            c.tokens,
            c.audience.join(","),
            c.terms.join(", "),
            c.text
        )
        .expect("write");
    }
    out
}

#[test]
fn fixture_page_gives_fixed_chunks() {
    let got = render();
    let path = fixtures().join("chunking.expected");
    if std::env::var_os("SPRIG_DOCS_REGEN").is_some() {
        std::fs::write(&path, &got).expect("write expected");
    }
    let want = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        got == want,
        "chunks differ from {}; rerun with SPRIG_DOCS_REGEN=1 to see the diff",
        path.display()
    );
}

#[test]
fn fixture_chunks_keep_the_rules() {
    let text = std::fs::read_to_string(fixtures().join("chunking.md")).expect("fixture");
    let page = parse_page("fixtures/chunking.md", &text).expect("page");
    let chunks = chunk_pages("0.0.0-test", &[page]);
    for c in &chunks {
        // Code fences are balanced in every chunk: never cut inside one.
        assert_eq!(c.text.matches("```").count() % 2, 0, "{}", c.heading_path);
        // The generated-page comment is gone.
        assert!(!c.text.contains("<!--"));
        // Every table part starts with the header.
        if let Some(i) = c.text.find("| `fixture.key_") {
            assert!(c.text[..i].contains("| Key | Type | Default | Description |\n|---|"));
        }
        assert!(
            usize::from(c.tokens) <= MAX_TOKENS + 60,
            "{}",
            c.heading_path
        );
    }
    let paths: Vec<&str> = chunks.iter().map(|c| c.heading_path.as_str()).collect();
    // Small sibling two merged into one; the tiny child into its sibling;
    // the empty parent and the empty tail make no chunk of their own.
    assert!(!paths.iter().any(|p| p.ends_with("Small sibling two")));
    assert!(!paths.iter().any(|p| p.ends_with("Tiny child")));
    assert!(
        !paths
            .iter()
            .any(|p| p.ends_with("Parent with children") || p.ends_with("Tail"))
    );
    // The long section and the long table are split, each piece keeping
    // the heading path.
    let long = paths
        .iter()
        .filter(|p| p.ends_with("> Long section"))
        .count();
    let table = paths.iter().filter(|p| p.ends_with("> Long table")).count();
    assert!(long >= 2 && table >= 2, "{paths:?}");
    // Ids are unique.
    let mut ids: Vec<_> = chunks.iter().map(|c| c.chunk_id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), chunks.len());
}
