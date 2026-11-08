//! cargo-fuzz target `lexer`; the body is `sprig_sql_fuzz::fuzz_lexer`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_sql_fuzz::fuzz_lexer(data));
