//! cargo-fuzz target `handshake_response`; the body is `sprig_proto_mysql_fuzz::fuzz_handshake_response`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_handshake_response(data));
