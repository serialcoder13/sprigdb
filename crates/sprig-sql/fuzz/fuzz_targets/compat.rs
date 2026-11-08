//! cargo-fuzz target `compat`; the body is `sprig_sql_fuzz::fuzz_compat`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_sql_fuzz::fuzz_compat(data));
