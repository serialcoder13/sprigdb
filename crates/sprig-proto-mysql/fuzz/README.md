# sprig-proto-mysql fuzz targets

cargo-fuzz targets for the MySQL and MariaDB wire codec. The oracle: no
panic, no hang, bounded memory, and decode -> encode -> decode stability
wherever the crate has an encoder.

| Target | What it feeds |
|---|---|
| `codec` | `PacketCodec` decode with plain, zlib and zstd framing, read in pieces; every packet re-encodes and decodes to itself |
| `handshake_v10` | `HandshakeV10::decode` (the greeting, as the backend client reads it) |
| `handshake_response` | `HandshakeResponse::decode`, `SslRequest`, `handshake::parse_client_hello`, `check_transport`, `negotiate`, `limit_attrs` |
| `err_packet` | `ErrPacket::decode`, `AuthSwitchRequest`, `AuthMoreData` |
| `ok_packet` | `OkPacket::decode` + `session_changes`, `EofPacket`, `Terminator` (EOF and OK forms), `client::decode_session_state` + `SessionTracker` |
| `column_def` | `ColumnDef::decode` with and without `MARIADB_CLIENT_EXTENDED_METADATA`, `PrepareOk`, the column count, `classify_response`, `classify_row` |
| `rows` | `decode_text_row`, `decode_binary_row_types` |
| `command` | `Command::parse` and `parse_with` (both personas, a fake `StmtLookup`) |
| `server_auth` | `auth::authenticate` with a scripted client and a fake `LoginFlow` (every method, TLS on and off, forced switch), `handshake::read_packet` |
| `client` | `client::MysqlClient::handshake` and its response state machine over an in-memory server stream |

Leading input bytes pick settings (capabilities, persona, column types);
each harness's doc comment in `src/lib.rs` gives its layout.

## Running

The crate is not a workspace member (it has its own `[workspace]` and
`Cargo.lock`), and cargo-fuzz needs nightly:

```sh
rustup toolchain install nightly --profile minimal   # once
cargo install cargo-fuzz                             # once
cd crates/sprig-proto-mysql/fuzz
cargo +nightly fuzz run codec work/codec corpus/codec regressions/codec -- -max_total_time=60
```

The first directory is where libFuzzer writes new inputs (`work/`, ignored
by git); `corpus/` and `regressions/` are read only.

Without nightly, the same harnesses replay every committed input on stable:

```sh
cargo test                                            # in this directory
cargo test -p sprig-proto-mysql --test golden_fuzz_replay   # from the workspace
```

The second includes `src/lib.rs` by path, so the corpora and regressions run
in the workspace's ordinary test run.

## Layout

- `src/lib.rs`: the harnesses (`fuzz_<target>`), shared by the cargo-fuzz
  binaries, the stable replay, and the workspace test.
- `fuzz_targets/<target>.rs`: one-line cargo-fuzz entry points.
- `corpus/<target>/golden-*`: seeds built from the golden captures
  (`tests/golden/`) by
  `SPRIG_WRITE_FUZZ_SEEDS=1 cargo test -p sprig-proto-mysql --test golden_fuzz_replay write_fuzz_seeds`.
- `regressions/<target>/`: every input that once failed, named for what it
  found. Never delete one: each guards a bug that was fixed.

## Licences

This crate is never built into a SprigDB binary and is outside the
workspace, so `cargo deny check` on the workspace doesn't see it.
`libfuzzer-sys` is MIT or Apache-2.0; the libFuzzer sources it bundles and
compiles are LLVM's, under Apache-2.0 WITH LLVM-exception. Both are
compatible with SprigDB's Apache-2.0, but `Apache-2.0 WITH LLVM-exception`
is not on `deny.toml`'s allow-list; if this crate ever joins the workspace,
add the exception there first.
