//! SQL text helpers: literals, identifiers and JSON paths (chapter 31,
//! "Security notes"). Values go into SQL only as escaped literals built
//! here; field paths become JSON path literals only after a strict check.

use std::fmt::Write as _;

/// A string literal: `'…'` with `\` and `'` escaped and NUL as `\0`
/// (gateway sessions run without `NO_BACKSLASH_ESCAPES`).
#[must_use]
pub fn str_lit(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            '\u{1a}' => o.push_str("\\Z"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// A binary literal `X'…'`.
#[must_use]
pub fn bin_lit(b: &[u8]) -> String {
    let mut o = String::with_capacity(b.len() * 2 + 3);
    o.push_str("X'");
    for x in b {
        let _ = write!(o, "{x:02X}");
    }
    o.push('\'');
    o
}

/// A backquoted identifier.
#[must_use]
pub fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Quotes one JSON path key: `"name"` with `"` and `\` escaped.
fn json_key(k: &str) -> String {
    let mut o = String::with_capacity(k.len() + 2);
    o.push('"');
    for c in k.chars() {
        if c == '"' || c == '\\' {
            o.push('\\');
        }
        o.push(c);
    }
    o.push('"');
    o
}

/// The JSON path of dotted field `parts`, with `[*]` after the parts
/// whose index is in `wild`: `$."a"[*]."b"`.
#[must_use]
pub fn json_path(parts: &[&str], wild: &[bool]) -> String {
    let mut o = String::from("$");
    for (i, p) in parts.iter().enumerate() {
        o.push('.');
        o.push_str(&json_key(p));
        if wild.get(i).copied().unwrap_or(false) {
            o.push_str("[*]");
        }
    }
    o
}

/// The `_types` key of a dotted path, as a JSON path: `$."a.b"`.
#[must_use]
pub fn types_path(path: &str) -> String {
    let key: Vec<String> = path.split('.').map(crate::json::escape_name).collect();
    format!("$.{}", json_key(&key.join(".")))
}

/// Whether `path` is a field path the translator may put into SQL: no
/// empty part, no `$` prefix, no NUL.
#[must_use]
pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\0')
        && path.split('.').all(|p| !p.is_empty() && !p.starts_with('$'))
}

/// A double as an SQL literal (finite only).
#[must_use]
pub fn f64_lit(f: f64) -> String {
    let s = format!("{f:?}");
    if s.contains(['e', 'E']) {
        s.replace('e', "E")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaping() {
        assert_eq!(str_lit("a'b\\c"), "'a''b\\\\c'");
        assert_eq!(bin_lit(&[0x1e, 0xff]), "X'1EFF'");
        assert_eq!(ident("a`b"), "`a``b`");
        assert_eq!(json_path(&["a", "b\"c"], &[true, false]), "$.\"a\"[*].\"b\\\"c\"");
        assert_eq!(types_path("items.0.price"), "$.\"items.0.price\"");
        assert!(valid_path("a.b"));
        assert!(!valid_path("a..b"));
        assert!(!valid_path("$a"));
        assert_eq!(f64_lit(1e300), "1E300");
        assert_eq!(f64_lit(5.0), "5.0");
    }
}
