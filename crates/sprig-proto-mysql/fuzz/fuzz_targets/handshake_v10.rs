//! cargo-fuzz target `handshake_v10`; the body is `sprig_proto_mysql_fuzz::fuzz_handshake_v10`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_handshake_v10(data));
