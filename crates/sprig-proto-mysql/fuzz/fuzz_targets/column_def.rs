//! cargo-fuzz target `column_def`; the body is `sprig_proto_mysql_fuzz::fuzz_column_def`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_column_def(data));
