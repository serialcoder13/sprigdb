//! `sprig-sql`: parser extensions on sqlparser-rs, the binder, name
//! virtualization and the translator.
//!
//! Layer: 2 execution. Design: [chapter 07](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/07-names.md)
//! (Name virtualization) and [chapter 28](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/28-translator.md)
//! (Translator) of the SprigDB low-level design.
//!
//! Implemented so far (chapter 07 build steps 1 to 5, 7 and 8):
//!
//! - [`lex`]: the persona-aware lexer, with executable comments
//!   ([`lex::versioned`]), `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`;
//! - [`names::map`]: the physical-name grammar and the per-tenant
//!   [`names::NameMap`];
//! - [`names::rewrite`](mod@names::rewrite): the statement rewrite over every name position,
//!   by span editing, with the `DATABASE()`, account-function and
//!   query-attribute substitutions;
//! - [`names::show`], [`names::infoschema`], [`names::output`],
//!   [`names::hide`], [`names::result`] and [`names::plan`](mod@names::plan):
//!   the `SHOW` family, `information_schema` inline views, names in
//!   results and messages, hiding of internal objects, and the per-statement
//!   plan the gateway follows;
//! - [`names::ddl`]: the typed DDL target of each statement and its
//!   conversion to `sprig-catalog`'s `DdlStatement` (chapter 08 step 10,
//!   the gateway side).
//! - [`settings_stmt`]: settings in SQL (chapter 08 step 8): `SPRIG SET`,
//!   `RESET`, `BOUNDS`, `SHOW SPRIG SETTINGS` and `ALTER TENANT <name>`.

pub mod lex;
pub mod names;
pub mod settings_stmt;
