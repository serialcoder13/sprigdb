//! JSON values as MySQL 8.4 prints them, on port 3306 (chapter 28,
//! "Output on port 3306", build step 11; gaps row 36).
//!
//! MariaDB stores JSON as the text it was given and returns that text;
//! MySQL stores a binary form and prints it canonically. [`canonical`]
//! re-prints a value MySQL's way (measured against MySQL 8.4.11 on 10
//! November 2026):
//!
//! - separators `", "` and `": "`, no other white space (`[ ]` is `[]`);
//! - object keys ordered by length, then by their bytes (`{"a": .., "b":
//!   .., "aa": ..}`), and a duplicate key keeps its last value;
//! - strings with their escapes decoded and written again as MySQL does:
//!   `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, `\u00XX` for other control
//!   characters, everything else (also `/` and non-ASCII) as itself.
//!
//! **Numbers keep their text.** MySQL re-prints doubles from their binary
//! value (`1e2` as `100.0`, `1e22` as `1e22`, `123456789.123456789` as
//! `123456789.1234568`), with rules of its `my_gcvt` that aren't
//! reproduced here; integers and plain decimals, the common case, print
//! the same either way.

/// `text` printed MySQL's way, or `None` when it isn't valid JSON (it is
/// then sent as it is).
#[must_use]
pub fn canonical(text: &[u8]) -> Option<Vec<u8>> {
    let mut p = P { s: text, i: 0 };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != text.len() {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() + 8);
    write(&v, &mut out);
    Some(out)
}

/// Whether [`canonical`] would change `text` (cheap pre-check: any white
/// space outside strings, an object, or an escape).
#[must_use]
pub fn may_change(text: &[u8]) -> bool {
    text.iter()
        .any(|&c| matches!(c, b'{' | b'\\' | b' ' | b'\t' | b'\n' | b'\r' | b','))
}

const MAX_DEPTH: usize = 128;

enum V<'a> {
    Lit(&'a [u8]),
    Str(Vec<u8>),
    Arr(Vec<V<'a>>),
    Obj(Vec<(Vec<u8>, V<'a>)>),
}

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> bool {
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Option<V<'a>> {
        if depth > MAX_DEPTH {
            return None;
        }
        match *self.s.get(self.i)? {
            b'{' => {
                self.i += 1;
                let mut members: Vec<(Vec<u8>, V<'a>)> = Vec::new();
                self.ws();
                if !self.eat(b'}') {
                    loop {
                        self.ws();
                        let k = self.string()?;
                        self.ws();
                        if !self.eat(b':') {
                            return None;
                        }
                        self.ws();
                        let v = self.value(depth + 1)?;
                        // The last of duplicate keys wins (MySQL 8.0.3+).
                        members.retain(|(mk, _)| *mk != k);
                        members.push((k, v));
                        self.ws();
                        if self.eat(b'}') {
                            break;
                        }
                        if !self.eat(b',') {
                            return None;
                        }
                    }
                }
                members.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(&b.0)));
                Some(V::Obj(members))
            }
            b'[' => {
                self.i += 1;
                let mut items = Vec::new();
                self.ws();
                if !self.eat(b']') {
                    loop {
                        self.ws();
                        items.push(self.value(depth + 1)?);
                        self.ws();
                        if self.eat(b']') {
                            break;
                        }
                        if !self.eat(b',') {
                            return None;
                        }
                    }
                }
                Some(V::Arr(items))
            }
            b'"' => Some(V::Str(self.string()?)),
            b't' | b'f' | b'n' => {
                for w in [&b"true"[..], b"false", b"null"] {
                    if self.s[self.i..].starts_with(w) {
                        let lit = &self.s[self.i..self.i + w.len()];
                        self.i += w.len();
                        return Some(V::Lit(lit));
                    }
                }
                None
            }
            b'-' | b'0'..=b'9' => {
                let start = self.i;
                self.eat(b'-');
                let digits = |p: &mut Self| {
                    let s = p.i;
                    while p.i < p.s.len() && p.s[p.i].is_ascii_digit() {
                        p.i += 1;
                    }
                    p.i > s
                };
                if !digits(self) {
                    return None;
                }
                if self.eat(b'.') && !digits(self) {
                    return None;
                }
                if self.eat(b'e') || self.eat(b'E') {
                    if !self.eat(b'+') {
                        self.eat(b'-');
                    }
                    if !digits(self) {
                        return None;
                    }
                }
                Some(V::Lit(&self.s[start..self.i]))
            }
            _ => None,
        }
    }

    /// A string's decoded bytes (UTF-8).
    fn string(&mut self) -> Option<Vec<u8>> {
        if !self.eat(b'"') {
            return None;
        }
        let mut out = Vec::new();
        loop {
            let c = *self.s.get(self.i)?;
            self.i += 1;
            match c {
                b'"' => return Some(out),
                b'\\' => {
                    let e = *self.s.get(self.i)?;
                    self.i += 1;
                    match e {
                        b'"' | b'\\' | b'/' => out.push(e),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                if !(self.eat(b'\\') && self.eat(b'u')) {
                                    return None;
                                }
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return None;
                                }
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            }
                            let ch = char::from_u32(cp)?;
                            let mut b = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut b).as_bytes());
                        }
                        _ => return None,
                    }
                }
                0..=0x1f => return None,
                _ => out.push(c),
            }
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let h = self.s.get(self.i..self.i + 4)?;
        self.i += 4;
        u32::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok()
    }
}

fn write_str(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            8 => out.extend_from_slice(b"\\b"),
            12 => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0..=0x1f => out.extend_from_slice(format!("\\u{c:04x}").as_bytes()),
            _ => out.push(c),
        }
    }
    out.push(b'"');
}

fn write(v: &V<'_>, out: &mut Vec<u8>) {
    match v {
        V::Lit(l) => out.extend_from_slice(l),
        V::Str(s) => write_str(s, out),
        V::Arr(items) => {
            out.push(b'[');
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                write(it, out);
            }
            out.push(b']');
        }
        V::Obj(members) => {
            out.push(b'{');
            for (i, (k, it)) in members.iter().enumerate() {
                if i > 0 {
                    out.extend_from_slice(b", ");
                }
                write_str(k, out);
                out.extend_from_slice(b": ");
                write(it, out);
            }
            out.push(b'}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> String {
        String::from_utf8(canonical(s.as_bytes()).expect(s)).unwrap()
    }

    /// Each pair was printed by MySQL 8.4.11 for the stored text.
    #[test]
    fn mysqls_form() {
        assert_eq!(
            c(r#"{"b":1,"a":[1,2,{"zz":true,"c":null}],"aa":"x"}"#),
            r#"{"a": [1, 2, {"c": null, "zz": true}], "b": 1, "aa": "x"}"#
        );
        assert_eq!(c(r#"{"a":1,"a":2}"#), r#"{"a": 2}"#);
        assert_eq!(c("  [ ]  "), "[]");
        assert_eq!(
            c(r#"{"k": {"y":{}, "x": []}}"#),
            r#"{"k": {"x": [], "y": {}}}"#
        );
        assert_eq!(c(r#""stré\n\/""#), "\"stré\\n/\"");
        assert_eq!(
            c("[1.5, -0.0, 1.0, 123456789012345678]"),
            "[1.5, -0.0, 1.0, 123456789012345678]"
        );
        assert_eq!(c(r#"{"test":true}"#), r#"{"test": true}"#);
        assert_eq!(c(r#""😀\u0001""#), "\"\u{1F600}\\u0001\"");
    }

    #[test]
    fn invalid_text_is_left_alone() {
        for s in ["{", "[1,]", "{'a':1}", "tru", "01x", "\"a\nb\"", ""] {
            assert!(canonical(s.as_bytes()).is_none(), "{s}");
        }
    }
}
