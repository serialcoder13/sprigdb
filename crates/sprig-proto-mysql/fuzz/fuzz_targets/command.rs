//! cargo-fuzz target `command`; the body is `sprig_proto_mysql_fuzz::fuzz_command`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| sprig_proto_mysql_fuzz::fuzz_command(data));
