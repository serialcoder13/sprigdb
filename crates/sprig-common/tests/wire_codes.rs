//! Every SprigDB error code a crate can send is in the registry
//! (`SprigCode`), so the generated error reference lists it.
//!
//! The test reads each crate's `src/` (not its tests, and not code after
//! `#[cfg(test)]`), with comments removed, and checks:
//! - every number 39000–39999 in the code is a registered code;
//! - every `ER_SPRIG_*` name is a registered name;
//! - every `<number> ER_SPRIG_<name>` pair in a message agrees with the
//!   registry.
//!
//! A protocol crate that sends a code the registry lacks fails here: add the
//! code to `SprigCode` (with its meaning, SQLSTATE and retry flag), or to
//! `SprigCode::RESERVED` if it is defined but not raised yet.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sprig_common::SprigCode;

/// The source with comments removed; string and char literals kept.
fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        if b[i..].starts_with(b"//") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            let mut depth = 0;
            while i < b.len() {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if b[i] == b'"' {
            let start = i;
            i += 1;
            while i < b.len() && b[i] != b'"' {
                i += if b[i] == b'\\' { 2 } else { 1 };
            }
            i = (i + 1).min(b.len());
            out.push_str(&src[start..i]);
        } else if b[i] == b'\'' {
            // A char literal ('"', '\'', 'x') or a lifetime.
            let end = if b.get(i + 1) == Some(&b'\\') {
                src[i + 2..].find('\'').map(|j| i + 2 + j + 1)
            } else if b.get(i + 2) == Some(&b'\'') {
                Some(i + 3)
            } else {
                None
            };
            let end = end.unwrap_or(i + 1).min(b.len());
            out.push_str(&src[i..end]);
            i = end;
        } else {
            let ch = src[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs")
            && p.file_name().is_some_and(|n| n != "tests.rs")
        {
            out.push(p);
        }
    }
}

/// Numbers 39000–39999 that stand alone (not part of a longer number or a
/// decimal).
fn codes_in(text: &str) -> Vec<u16> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 5 <= b.len() {
        let before_ok =
            i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'.' || b[i - 1] == b'_');
        let five = &b[i..i + 5];
        let after_ok = b
            .get(i + 5)
            .is_none_or(|c| !(c.is_ascii_digit() || *c == b'_' || *c == b'.'));
        if before_ok
            && five[0] == b'3'
            && five[1] == b'9'
            && five.iter().all(u8::is_ascii_digit)
            && after_ok
        {
            out.push(std::str::from_utf8(five).unwrap().parse().unwrap());
            i += 5;
        } else {
            i += 1;
        }
    }
    out
}

/// `ER_SPRIG_*` names, with the 39xxx number written just before, if any.
fn names_in(text: &str) -> Vec<(Option<u16>, String)> {
    let mut out = Vec::new();
    let mut rest = text;
    let mut offset = 0;
    while let Some(j) = rest.find("ER_SPRIG_") {
        let at = offset + j;
        let name: String = text[at..]
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        let name = name.trim_end_matches('_').to_owned();
        let before = &text[..at];
        let num = before
            .strip_suffix(' ')
            .filter(|b| b.len() >= 5)
            .map(|b| &b[b.len() - 5..])
            .and_then(|d| d.parse::<u16>().ok())
            .filter(|n| (39000..=39999).contains(n));
        if name.len() > "ER_SPRIG_".len() {
            out.push((num, name.clone()));
        }
        offset = at + name.len().max(1);
        rest = &text[offset..];
    }
    out
}

#[test]
fn every_code_sent_on_the_wire_is_registered() {
    let crates = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for e in std::fs::read_dir(&crates).unwrap() {
        let dir = e.unwrap().path();
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        // The documentation crates quote codes from the registry itself.
        if name.starts_with("sprig-docs") {
            continue;
        }
        if dir.join("src").is_dir() {
            rust_files(&dir.join("src"), &mut files);
        }
    }
    assert!(files.len() > 100, "found only {} source files", files.len());
    let registered: BTreeSet<u16> = SprigCode::ALL.iter().map(|c| c.number()).collect();
    let names: BTreeSet<&str> = SprigCode::ALL.iter().map(|c| c.name()).collect();
    let mut problems = Vec::new();
    for f in &files {
        let src = std::fs::read_to_string(f).unwrap();
        let code = strip_comments(&src);
        let code = code.split("#[cfg(test)]").next().unwrap_or_default();
        let rel = f.strip_prefix(&crates).unwrap_or(f).display().to_string();
        for n in codes_in(code) {
            if !registered.contains(&n) {
                problems.push(format!("{rel}: {n} isn't in SprigCode"));
            }
        }
        for (num, name) in names_in(code) {
            if !names.contains(name.as_str()) {
                problems.push(format!("{rel}: {name} isn't in SprigCode"));
            } else if let Some(n) = num
                && SprigCode::from_number(n).map(SprigCode::name) != Some(name.as_str())
            {
                problems.push(format!("{rel}: \"{n} {name}\" disagrees with SprigCode"));
            }
        }
    }
    problems.sort();
    problems.dedup();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn reserved_codes_are_registered_once_with_a_note() {
    let mut seen = BTreeSet::new();
    for (code, note) in SprigCode::RESERVED {
        assert!(seen.insert(code.number()), "{code:?} listed twice");
        assert!(note.starts_with("Reserved: "), "{code:?}: {note}");
        assert_eq!(code.reserved(), Some(*note));
    }
    assert_eq!(SprigCode::CursorLimit.reserved(), None);
}

#[test]
fn the_scanner_finds_codes_and_names() {
    assert_eq!(
        codes_in("x 39840, 139840 39840.5 v39841 39842"),
        vec![39840, 39842]
    );
    let n = names_in(r#""39940 ER_SPRIG_STREAM_NOT_FOUND: no" and ER_SPRIG_X_Y"#);
    assert_eq!(
        n,
        vec![
            (Some(39940), "ER_SPRIG_STREAM_NOT_FOUND".to_owned()),
            (None, "ER_SPRIG_X_Y".to_owned())
        ]
    );
    assert_eq!(
        strip_comments("a // 39001\nb /* 39002 */ \"// 39003\""),
        "a \nb  \"// 39003\""
    );
}
