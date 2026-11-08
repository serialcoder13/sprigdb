//! cargo-fuzz target `codec`; the body is `sprig_proto_mysql_fuzz::fuzz_codec`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_codec(data));
