//! cargo-fuzz target `rewriter`; the body is `sprig_sql_fuzz::fuzz_rewriter`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_sql_fuzz::fuzz_rewriter(data));
