//! cargo-fuzz target `client`; the body is `sprig_proto_mysql_fuzz::fuzz_client`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_client(data));
