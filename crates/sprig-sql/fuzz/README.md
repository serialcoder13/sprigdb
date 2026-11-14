# sprig-sql fuzz targets

cargo-fuzz targets for the SQL layer: the persona-aware lexer, the name
rewriter and plan, and the translator's core rewrites. The wire codec's targets
are in `../../sprig-proto-mysql/fuzz`. The oracle is no panic, no hang and
bounded memory, plus each layer's invariants, listed in `src/lib.rs`.

| Target | What it feeds |
|---|---|
| `lexer` | `lex::lex` on both personas with `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`: tokens cover every byte in order; `neutralize_edits` offsets land on a `!`; `expand_for_mariadb` output lexes |
| `rewriter` | `names::plan` for a tenant with awkward database names and a sharded database, both engines, `lower_case_table_names`; each forwarded statement lexes and unrewrites, and its `ResultMapper` runs over rows made of the input |
| `compat` | `compat::mysql_text_rewrites` (executable comments, `CAST(? AS JSON)`), `compat::vars::rewrite_reads`, `schema::describe_target`, `schema::mysql_create_table`, `schema::show_columns_sql`, the collation map, `errmap::to_mysql`, and `meta::mysql_column` on decoded column definitions |

The first input byte picks the settings: bit 0 the persona (MySQL, MariaDB),
bit 1 `ANSI_QUOTES`, bit 2 `NO_BACKSLASH_ESCAPES`, bit 3 the AST engine,
bit 4 `lower_case_table_names`.

## Running

The crate is not a workspace member (its own `[workspace]` and `Cargo.lock`),
and cargo-fuzz needs nightly:

```sh
cd crates/sprig-sql/fuzz
cargo +nightly fuzz run -O lexer work/lexer corpus/lexer -- -max_total_time=60
```

`work/` is where libFuzzer writes new inputs (git-ignored); `corpus/` and
`regressions/<target>/` are read only. CI's `S1 fuzz smoke` job runs every
target of both fuzz crates for `FUZZ_SECONDS` (30) each.

Without nightly, the same harnesses replay every committed input on stable:

```sh
cargo test                                   # in this directory
cargo test -p sprig-sql --test fuzz_replay   # from the workspace
```

The second includes `src/lib.rs` by path. `SPRIG_WRITE_FUZZ_SEEDS=1 cargo test
-p sprig-sql --test fuzz_replay write_fuzz_seeds` rewrites `corpus/*/seed-*`
from the statements listed there. Every input that once failed goes into
`regressions/<target>/`, named for what it found, and is never deleted.

## Licences

As for `sprig-proto-mysql/fuzz`: `libfuzzer-sys` is MIT or Apache-2.0 and
bundles LLVM's libFuzzer (Apache-2.0 WITH LLVM-exception). The crate is never
built into a SprigDB binary and is outside the workspace, so `cargo deny` on
the workspace doesn't see it.
