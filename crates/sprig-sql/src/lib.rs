//! `sprig-sql`: parser extensions on sqlparser-rs, the binder, name
//! virtualization and the translator.
//!
//! Layer: 2 execution.
//!
//! Modules:
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
//!   conversion to `sprig-catalog`'s `DdlStatement` (the gateway
//!   side).
//! - [`settings_stmt`]: settings in SQL: `SPRIG SET`, `RESET`, `BOUNDS`, `SHOW
//!   SPRIG SETTINGS` and `ALTER TENANT <name>`.
//! - [`pin_stmt`]: sharding pins and advice: `SPRIG PIN`, `SPRIG UNPIN`, `SPRIG
//!   SHOW ADVICE` and `SPRIG SHOW PINS`.
//! - [`compat`]: the MySQL/MariaDB translator: executable comments, emulated
//!   variables, the collation map, the persona-shaped `information_schema`
//!   providers and `SHOW` rewrites, result metadata and error mapping on 3306.

pub mod access;
pub mod account_stmt;
pub mod compat;
pub mod lex;
pub mod names;
pub mod pin_stmt;
// Recommendations in SQL.
pub mod rec_stmt;
// Row-level security (policies and the rewrite).
pub mod rls;
pub mod settings_stmt;
// What stored code's text names.
pub mod stored_refs;
