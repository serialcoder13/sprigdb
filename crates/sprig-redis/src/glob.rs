//! Redis's glob matcher (`stringmatchlen`), for `KEYS`, `SCAN MATCH`,
//! `PSUBSCRIBE` and the like: `*`, `?`, `[...]` with `^` and ranges, `\`
//! escapes; bytes compared exactly.

/// Whether `s` matches `pattern`.
#[must_use]
pub fn matches(pattern: &[u8], s: &[u8]) -> bool {
    // `*` alone matches everything, the empty string too (Redis's
    // `allkeys` shortcut).
    if pattern == b"*" {
        return true;
    }
    let mut skip = 0u32;
    m(pattern, s, false, &mut skip, 0)
}

/// Whether `s` matches `pattern`, ignoring ASCII case.
#[must_use]
pub fn matches_nocase(pattern: &[u8], s: &[u8]) -> bool {
    let mut skip = 0u32;
    m(pattern, s, true, &mut skip, 0)
}

fn eq(a: u8, b: u8, nocase: bool) -> bool {
    if nocase {
        a.eq_ignore_ascii_case(&b)
    } else {
        a == b
    }
}

#[allow(clippy::too_many_lines)]
fn m(mut p: &[u8], mut s: &[u8], nocase: bool, skip: &mut u32, nesting: u32) -> bool {
    // Redis bounds recursion the same way (stringmatchlen_impl).
    if nesting > 1000 {
        return false;
    }
    while !p.is_empty() && !s.is_empty() {
        match p[0] {
            b'*' => {
                while p.len() > 1 && p[1] == b'*' {
                    p = &p[1..];
                }
                if p.len() == 1 {
                    return true;
                }
                while !s.is_empty() {
                    if m(&p[1..], s, nocase, skip, nesting + 1) {
                        return true;
                    }
                    if *skip > 0 {
                        return false;
                    }
                    s = &s[1..];
                }
                *skip = 1;
                return false;
            }
            b'?' => {
                s = &s[1..];
            }
            b'[' => {
                p = &p[1..];
                let not = p.first() == Some(&b'^');
                if not {
                    p = &p[1..];
                }
                let mut hit = false;
                loop {
                    if p.is_empty() {
                        // Redis: an unterminated class ends the pattern.
                        break;
                    }
                    if p[0] == b'\\' && p.len() >= 2 {
                        p = &p[1..];
                        if eq(p[0], s[0], nocase) {
                            hit = true;
                        }
                    } else if p[0] == b']' {
                        break;
                    } else if p.len() >= 3 && p[1] == b'-' {
                        let (mut lo, mut hi) = (p[0], p[2]);
                        let mut c = s[0];
                        if lo > hi {
                            std::mem::swap(&mut lo, &mut hi);
                        }
                        if nocase {
                            lo = lo.to_ascii_lowercase();
                            hi = hi.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        p = &p[2..];
                        if c >= lo && c <= hi {
                            hit = true;
                        }
                    } else if eq(p[0], s[0], nocase) {
                        hit = true;
                    }
                    p = &p[1..];
                }
                if p.is_empty() {
                    // Pattern ended inside the class: Redis steps back one.
                    let hit = if not { !hit } else { hit };
                    if !hit {
                        return false;
                    }
                    s = &s[1..];
                    // p is empty: finish below.
                    continue;
                }
                let hit = if not { !hit } else { hit };
                if !hit {
                    return false;
                }
                s = &s[1..];
            }
            b'\\' if p.len() >= 2 => {
                p = &p[1..];
                if !eq(p[0], s[0], nocase) {
                    return false;
                }
                s = &s[1..];
            }
            c => {
                if !eq(c, s[0], nocase) {
                    return false;
                }
                s = &s[1..];
            }
        }
        if !p.is_empty() {
            p = &p[1..];
        }
        if s.is_empty() {
            while p.first() == Some(&b'*') {
                p = &p[1..];
            }
            break;
        }
    }
    p.is_empty() && s.is_empty()
}

/// The literal prefix of `pattern` (before any `*`, `?`, `[` or `\`), for
/// pushing a byte range down to SQL.
#[must_use]
pub fn literal_prefix(pattern: &[u8]) -> &[u8] {
    let end = pattern
        .iter()
        .position(|c| matches!(c, b'*' | b'?' | b'[' | b'\\'))
        .unwrap_or(pattern.len());
    &pattern[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_globs() {
        assert!(matches(b"*", b""));
        assert!(matches(b"*", b"abc"));
        assert!(matches(b"h?llo", b"hello"));
        assert!(!matches(b"h?llo", b"hllo"));
        assert!(matches(b"h*llo", b"heeeello"));
        assert!(matches(b"h[ae]llo", b"hallo"));
        assert!(!matches(b"h[^e]llo", b"hello"));
        assert!(matches(b"h[a-b]llo", b"hbllo"));
        assert!(matches(b"user:*:name", b"user:42:name"));
        assert!(!matches(b"user:*:name", b"user:42:age"));
        assert!(matches(b"a\\*b", b"a*b"));
        assert!(!matches(b"a\\*b", b"axb"));
        assert!(matches(b"*a*b*", b"xxaxxbxx"));
        assert!(matches_nocase(b"HELLO", b"hello"));
        assert!(!matches(b"abc", b"abcd"));
        assert!(matches(b"abc*", b"abc"));
        assert_eq!(literal_prefix(b"user:*"), b"user:");
    }
}
