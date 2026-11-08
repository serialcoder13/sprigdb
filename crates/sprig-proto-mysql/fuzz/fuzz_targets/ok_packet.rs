//! cargo-fuzz target `ok_packet`; the body is `sprig_proto_mysql_fuzz::fuzz_ok_packet`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_ok_packet(data));
