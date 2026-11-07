//! Skipping tests whose environment isn't configured.
//!
//! Integration tests need real services: etcd, MariaDB in some topology,
//! Docker. Each one names the variable that points at its service and
//! returns early, with a message, when the variable isn't set, so
//! `cargo test` on a laptop without Compose stays green. The variables hold
//! connection URLs; a variable naming several servers holds a
//! comma-separated list, writer first (see [`urls`]).
//!
//! | Variable | Holds | Example |
//! |---|---|---|
//! | [`ETCD`] | etcd client URLs | `http://127.0.0.1:2379` |
//! | [`MARIADB_SINGLE`] | one MariaDB URL | `mysql://root:sprig-dev-root@127.0.0.1:13301` |
//! | [`MARIADB_REPLICATION`] | primary, then replicas | `mysql://…@127.0.0.1:13311,mysql://…@127.0.0.1:13312` |
//! | [`MARIADB_GALERA`] | the Galera nodes | `mysql://…@127.0.0.1:13321,…` |
//! | [`DOCKER`] | `1` when tests may run `docker compose` | `1` |
//! | [`ENV_FILE`] | a TOML [`Env`](crate::Env) file | `deploy/compose/env.3dc.toml` |
//!
//! Rust has no runtime "skipped" status, so a skipped test reports as
//! passed. CI sets [`STRICT`] in stages that provide the services: there a
//! missing variable fails the test instead, so a typo can't hide a suite.

/// etcd client URLs (single-etcd mode, chapter 23).
pub const ETCD: &str = "SPRIG_TEST_ETCD";
/// One single-node MariaDB.
pub const MARIADB_SINGLE: &str = "SPRIG_TEST_MARIADB_SINGLE";
/// A primary with replicas: the primary's URL first.
pub const MARIADB_REPLICATION: &str = "SPRIG_TEST_MARIADB_REPLICATION";
/// A Galera cluster: one URL per node.
pub const MARIADB_GALERA: &str = "SPRIG_TEST_MARIADB_GALERA";
/// Set to `1` to allow tests that drive `docker compose` themselves.
pub const DOCKER: &str = "SPRIG_TEST_DOCKER";
/// Path of a TOML file describing the whole environment ([`crate::Env::load`]).
pub const ENV_FILE: &str = "SPRIG_TEST_ENV_FILE";
/// Set to `1` to turn a missing variable into a test failure (CI).
pub const STRICT: &str = "SPRIG_TEST_STRICT";
/// Set to `1` to keep Compose projects running after a test, for debugging.
pub const KEEP: &str = "SPRIG_TEST_KEEP";
/// Directory where Compose fixtures write logs of failed tests.
pub const LOG_DIR: &str = "SPRIG_TEST_LOG_DIR";

/// The value of `name`, trimmed, or `None` when it is unset or empty.
#[must_use]
pub fn var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// Whether `name` is set to a true value (`1`, `true`, `yes` or `on`).
#[must_use]
pub fn flag(name: &str) -> bool {
    var(name).is_some_and(|v| is_true(&v))
}

/// Whether strict mode is on ([`STRICT`]).
#[must_use]
pub fn strict() -> bool {
    flag(STRICT)
}

/// Splits a variable's value into URLs: comma-separated, whitespace
/// trimmed, empty items dropped.
#[must_use]
pub fn urls(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Reports that a test is skipped because `name` is unset; called by
/// [`require_env!`](crate::require_env).
///
/// # Panics
/// In strict mode ([`STRICT`]), so CI notices the missing variable.
pub fn report_missing(name: &str, test: &str) {
    assert!(
        !strict(),
        "{test}: {name} is not set and {STRICT} is on; set {name} or unset {STRICT}"
    );
    eprintln!("skipped {test}: set {name} to run it (see sprig_testkit::skip)");
}

fn is_true(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

/// Evaluates to the value of an environment variable, or skips the test.
///
/// When the variable is unset or empty, prints `skipped <module>: set <VAR>
/// to run it` and returns from the enclosing function: with `()` by default,
/// or with the second argument, such as `Ok(())` in a test that returns a
/// `Result`. In strict mode (`SPRIG_TEST_STRICT=1`) it panics instead.
///
/// ```no_run
/// use sprig_testkit::{require_env, skip};
///
/// #[test]
/// fn talks_to_galera() -> Result<(), String> {
///     let urls = skip::urls(&require_env!(skip::MARIADB_GALERA, Ok(())));
///     assert_eq!(urls.len(), 3);
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! require_env {
    ($name:expr) => {
        $crate::require_env!($name, ())
    };
    ($name:expr, $ret:expr) => {
        match $crate::skip::var($name) {
            ::std::option::Option::Some(v) => v,
            ::std::option::Option::None => {
                $crate::skip::report_missing($name, ::std::module_path!());
                return $ret;
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_and_trim() {
        assert_eq!(
            urls(" mysql://a:1 , ,mysql://b:2,"),
            vec!["mysql://a:1".to_owned(), "mysql://b:2".to_owned()]
        );
        assert!(urls("").is_empty());
    }

    #[test]
    fn truthy_values() {
        for v in ["1", "true", "YES", "on"] {
            assert!(is_true(v), "{v}");
        }
        for v in ["0", "false", "no", "off", "2"] {
            assert!(!is_true(v), "{v}");
        }
    }

    fn gated() -> Option<String> {
        let v = require_env!("SPRIG_TEST_SURELY_UNSET_VARIABLE_FOR_TESTKIT", None);
        Some(v)
    }

    #[test]
    fn require_env_returns_early_when_unset() {
        // Strict mode would turn the skip into a panic; this test checks the
        // non-strict path only.
        if strict() {
            return;
        }
        assert_eq!(gated(), None);
    }
}
