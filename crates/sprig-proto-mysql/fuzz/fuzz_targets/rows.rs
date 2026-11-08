//! cargo-fuzz target `rows`; the body is `sprig_proto_mysql_fuzz::fuzz_rows`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_rows(data));
