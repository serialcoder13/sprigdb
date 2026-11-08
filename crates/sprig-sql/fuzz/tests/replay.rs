//! Replays the committed corpora and crash regressions on stable, without
//! libFuzzer: `cargo test` in `crates/sprig-sql/fuzz`.

#[test]
fn corpora_and_regressions_replay_cleanly() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let n = sprig_sql_fuzz::replay_dirs(root, &["corpus", "regressions"]);
    assert!(n > 0, "no corpus files found under {}", root.display());
}
