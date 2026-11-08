//! `sprig-sql`: parser extensions on sqlparser-rs, the binder, name
//! virtualization and the translator.
//!
//! Layer: 2 execution. Design: [chapter 07](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/07-names.md)
//! (Name virtualization) and [chapter 28](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/28-translator.md)
//! (Translator) of the SprigDB low-level design.
//!
//! Implemented so far (chapter 07 build steps 1 to 3):
//!
//! - [`lex`]: the persona-aware lexer, with executable comments
//!   ([`lex::versioned`]), `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`;
//! - [`names::map`]: the physical-name grammar and the per-tenant
//!   [`names::NameMap`];
//! - [`names::rewrite`]: the statement rewrite over every name position,
//!   by span editing, with the `DATABASE()`, account-function and
//!   query-attribute substitutions.

pub mod lex;
pub mod names;
