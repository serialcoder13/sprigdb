//! cargo-fuzz target `server_auth`; the body is `sprig_proto_mysql_fuzz::fuzz_server_auth`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_server_auth(data));
