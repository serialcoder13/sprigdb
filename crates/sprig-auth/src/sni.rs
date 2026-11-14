//! `listener.tenant_sni_suffixes`: the DNS suffixes under which a TLS server
//! name names a tenant, so `app` connecting to `acme.db.example.com` logs
//! in to tenant `acme` without a dotted user name.
//!
//! - **Value.** Setting values have no list type, so the key is text: a
//!   comma-separated list (`db.example.com, sprig.example.net`), or the
//!   same as a JSON array of strings (`["db.example.com"]`, the form the
//!   book's `[]` default suggests). Empty: no tenant endpoints.
//! - **Validation.** Each suffix is a DNS name: labels of `a-z`, `0-9` and
//!   `-` (not first or last), 1–63 characters each, 253 in all; compared
//!   in lower case, a leading or trailing `.` dropped; no repeats.
//!   [`SniSuffixGuard`] refuses a bad value when it is written (39080);
//!   a front end that still meets one keeps its old list and raises
//!   `SettingNotApplied`.
//! - **Scope and apply.** `dc` and `global`; live: the next TLS login
//!   uses the new list.

use std::borrow::Cow;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Apply, GuardFuture, HardBounds, Scopes, Setting, Value, ValueGuard};

/// `listener.tenant_sni_suffixes`.
pub static TENANT_SNI_SUFFIXES: Setting<Cow<'static, str>> = Setting {
    key: "listener.tenant_sni_suffixes",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "Tenant endpoints by TLS SNI: comma-separated DNS suffixes; <tenant>.<suffix> names the tenant",
};
sprig_settings::register!(TENANT_SNI_SUFFIXES);

fn valid_label(l: &str) -> bool {
    !l.is_empty()
        && l.len() <= 63
        && !l.starts_with('-')
        && !l.ends_with('-')
        && l.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The suffixes `text` names, normalized (see the module docs).
///
/// # Errors
/// Why the value isn't a list of DNS suffixes.
pub fn parse(text: &str) -> std::result::Result<Vec<String>, String> {
    let t = text.trim();
    let items: Vec<String> = if t.starts_with('[') {
        serde_json::from_str::<Vec<String>>(t)
            .map_err(|e| format!("not a JSON array of strings: {e}"))?
    } else {
        t.split(',').map(str::to_owned).collect()
    };
    let mut out: Vec<String> = Vec::new();
    for raw in items {
        let s = raw.trim();
        if s.is_empty() && !t.starts_with('[') {
            continue;
        }
        let n = s.trim_matches('.').to_ascii_lowercase();
        if n.is_empty() || n.len() > 253 || !n.split('.').all(valid_label) {
            return Err(format!("{s:?} is not a DNS suffix"));
        }
        if out.contains(&n) {
            return Err(format!("{n:?} is listed twice"));
        }
        out.push(n);
    }
    Ok(out)
}

/// Refuses a `listener.tenant_sni_suffixes` value that isn't a list of DNS
/// suffixes (39080), before it is written.
#[derive(Debug, Clone, Copy, Default)]
pub struct SniSuffixGuard;

impl SniSuffixGuard {
    /// The check, synchronously.
    ///
    /// # Errors
    /// 39080 for a bad value of the key.
    pub fn check_value(key: &str, value: &Value) -> Result<()> {
        if key != TENANT_SNI_SUFFIXES.key {
            return Ok(());
        }
        let Value::Str(s) = value else {
            return Ok(()); // the key's type check refuses it
        };
        parse(s).map(|_| ()).map_err(|e| {
            SprigError::new(
                SprigCode::SettingOutOfBounds,
                format!("{}: {e}", TENANT_SNI_SUFFIXES.key),
            )
        })
    }
}

impl ValueGuard for SniSuffixGuard {
    fn check<'a>(&'a self, key: &'a str, value: &'a Value) -> GuardFuture<'a> {
        Box::pin(async move { Self::check_value(key, value) })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn lists_parse() {
        assert_eq!(parse("").unwrap(), Vec::<String>::new());
        assert_eq!(parse("  ").unwrap(), Vec::<String>::new());
        assert_eq!(
            parse("DB.Example.com., .sprig.example.net").unwrap(),
            vec!["db.example.com", "sprig.example.net"]
        );
        assert_eq!(
            parse(r#"["a.test","b-1.test"]"#).unwrap(),
            vec!["a.test", "b-1.test"]
        );
        assert_eq!(parse("[]").unwrap(), Vec::<String>::new());
        assert_eq!(parse("localhost").unwrap(), vec!["localhost"]);
    }

    #[test]
    fn bad_lists_are_refused() {
        for bad in [
            "a..b",
            "-a.test",
            "a-.test",
            "a_b.test",
            "a.test,a.test",
            "x y",
            r#"["ok.test", ""]"#,
            "[1]",
            &format!("{}.test", "a".repeat(64)),
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn guard_checks_only_its_key() {
        let bad = Value::Str("a..b".into());
        let e = SniSuffixGuard::check_value(TENANT_SNI_SUFFIXES.key, &bad).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SettingOutOfBounds));
        assert!(SniSuffixGuard::check_value("other.key", &bad).is_ok());
        assert!(
            SniSuffixGuard::check_value(TENANT_SNI_SUFFIXES.key, &Value::Str("db.test".into()))
                .is_ok()
        );
    }
}
