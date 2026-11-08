//! Substitutions (chapter 07 build step 3): `DATABASE()` and `SCHEMA()`,
//! the account functions, and `mysql_query_attribute_string()` (chapter 05,
//! "Query attributes").
//!
//! | Expression | Becomes |
//! |---|---|
//! | `DATABASE()`, `SCHEMA()` | `'billing'`, or `NULL` with no current database |
//! | `CURRENT_USER`, `CURRENT_USER()` | `'alice@%'`: user and the matched account's host pattern |
//! | `USER()`, `SESSION_USER()`, `SYSTEM_USER()` | `'alice@203.0.113.7'`: user and the client's address |
//! | `mysql_query_attribute_string('n')` (3306) | the attribute's value as a literal, or `NULL` |
//!
//! Substitutions are made in expressions of statements that run now. They
//! are not made inside the bodies of stored programs and views, which keep
//! backend names in R0 (step 12 rewrites them at `CREATE` time in R4), nor
//! in account statements (`GRANT`, `CREATE USER`, `SET PASSWORD FOR
//! CURRENT_USER()`, ...) and `DEFINER` clauses, where `CURRENT_USER` names
//! an account rather than a value (chapter 29, step 9).

/// A string literal holding `s` that reads back as `s` in any `sql_mode`:
/// single quotes (strings even under `ANSI_QUOTES`), a quote doubled, and
/// backslashes doubled unless `NO_BACKSLASH_ESCAPES`.
#[must_use]
pub fn sql_string(s: &str, no_backslash_escapes: bool) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' if !no_backslash_escapes => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// A built-in function the rewrite replaces, by its upper-case name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Builtin {
    /// `DATABASE()`, `SCHEMA()`.
    Database,
    /// `CURRENT_USER()`.
    CurrentUser,
    /// `USER()`, `SESSION_USER()`, `SYSTEM_USER()`.
    User,
    /// `mysql_query_attribute_string(name)`.
    QueryAttribute,
    /// `NEXTVAL(seq)`, `LASTVAL(seq)`, `SETVAL(seq, ...)`: the first
    /// argument is a sequence name (MariaDB).
    SequenceFn,
}

impl Builtin {
    /// Recognizes a function name.
    pub(super) fn parse(name: &[u8]) -> Option<Self> {
        const TABLE: [(&[u8], Builtin); 10] = [
            (b"DATABASE", Builtin::Database),
            (b"SCHEMA", Builtin::Database),
            (b"CURRENT_USER", Builtin::CurrentUser),
            (b"USER", Builtin::User),
            (b"SESSION_USER", Builtin::User),
            (b"SYSTEM_USER", Builtin::User),
            (b"MYSQL_QUERY_ATTRIBUTE_STRING", Builtin::QueryAttribute),
            (b"NEXTVAL", Builtin::SequenceFn),
            (b"LASTVAL", Builtin::SequenceFn),
            (b"SETVAL", Builtin::SequenceFn),
        ];
        TABLE
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|&(_, b)| b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literals_read_back_in_every_mode() {
        assert_eq!(sql_string("billing", false), "'billing'");
        assert_eq!(sql_string("o'neil", false), "'o''neil'");
        assert_eq!(sql_string(r"a\b", false), r"'a\\b'");
        assert_eq!(sql_string(r"a\b", true), r"'a\b'");
        assert_eq!(sql_string("", true), "''");
    }

    #[test]
    fn builtins_ignore_case() {
        assert_eq!(Builtin::parse(b"database"), Some(Builtin::Database));
        assert_eq!(Builtin::parse(b"Schema"), Some(Builtin::Database));
        assert_eq!(Builtin::parse(b"system_user"), Some(Builtin::User));
        assert_eq!(
            Builtin::parse(b"mysql_query_attribute_string"),
            Some(Builtin::QueryAttribute)
        );
        assert_eq!(Builtin::parse(b"now"), None);
    }
}
