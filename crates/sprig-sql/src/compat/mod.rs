//! The translator's R0 subset (chapter 28, build steps 1 to 5): what lets
//! MySQL and MariaDB clients connect and introspect through either persona.
//!
//! - Step 1, executable comments per persona: [`crate::lex::versioned`]
//!   (the lexer's rules and neutralization) and
//!   [`crate::lex::versioned::expand_for_mariadb`] (MySQL-only comments whose
//!   body MariaDB understands).
//! - Step 2, emulated variables and version strings: [`vars`].
//! - Step 3, the collation map with metadata ids: [`collation`].
//! - Step 4, the core `information_schema` providers and `SHOW` rewrites:
//!   [`schema`] (persona shapes on top of chapter 07's inline views), and
//!   [`meta`] for result metadata (MySQL's `JSON` type, 245).
//! - Step 5, error mapping: [`errmap`].
//!
//! The rule framework, translation cache and the R4 rule catalogue come
//! with step 6 (`sprig_sql::translate`).

pub mod collation;
pub mod errmap;
pub mod keywords;
pub mod meta;
pub mod schema;
pub mod vars;
