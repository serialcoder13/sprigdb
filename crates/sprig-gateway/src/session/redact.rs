//! Secrets out of a session's `Info` text (`SHOW PROCESSLIST`,
//! `information_schema.PROCESSLIST`, Studio's and the Admin API's session
//! lists).
//!
//! The text a client sent is shown while its statement runs, so a
//! password or a connection's credentials written as a literal would be
//! visible to every session that may list the tenant's processes. As
//! MySQL 8.4 does, such literals are shown as `<secret>`:
//!
//! - `IDENTIFIED BY 'x'`, `IDENTIFIED WITH p BY 'x'`, `IDENTIFIED ... AS
//!   'hash'`, `... REPLACE 'current'`, MariaDB's `IDENTIFIED VIA p USING 'x'`;
//! - `PASSWORD 'x'`, `PASSWORD = 'x'`, `PASSWORD('x')` (and `OLD_PASSWORD`,
//!   `MASTER_PASSWORD = 'x'`, any word ending in `PASSWORD`);
//! - every value in `CREDENTIALS (k = 'v', ...)` (`CREATE CONNECTION` and `ALTER CONNECTION ... SET CREDENTIALS`).
//!
//! A string cut off by the 1,024-character limit counts as a literal to
//! the end. Text without `IDENTIFIED`, `PASSWORD` or `CREDENTIALS` is
//! returned as it is, without scanning.

use std::borrow::Cow;

/// What a secret literal is shown as.
pub const SECRET: &str = "<secret>";

fn has_word(text: &str, w: &str) -> bool {
    text.as_bytes()
        .windows(w.len())
        .any(|x| x.eq_ignore_ascii_case(w.as_bytes()))
}

/// The end (exclusive) of the quoted token starting at `i` (`'`, `"` or
/// `` ` ``): a doubled quote or (outside backquotes) a backslash escapes.
fn quoted_end(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == b'\\' && q != b'`' {
            j += 2;
            continue;
        }
        if b[j] == q {
            if b.get(j + 1) == Some(&q) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    b.len()
}

#[derive(Clone, PartialEq)]
enum Tok {
    Word(String),
    Punct(u8),
    Other,
}

/// `text` with its secret literals replaced by [`SECRET`].
#[must_use]
#[allow(clippy::too_many_lines)] // one scanner loop, one arm per token kind
pub fn info(text: &str) -> Cow<'_, str> {
    if !(has_word(text, "IDENTIFIED")
        || has_word(text, "PASSWORD")
        || has_word(text, "CREDENTIALS"))
    {
        return Cow::Borrowed(text);
    }
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    let mut changed = false;
    // The two significant tokens before the current one, nearest first.
    let mut prev: [Tok; 2] = [Tok::Other, Tok::Other];
    let mut identified = false;
    // Parenthesis depth inside `CREDENTIALS (...)`, 0 outside.
    let mut cred = 0usize;
    let mut cred_next = false;
    // Words since the statement began, and whether it is `SET PASSWORD`
    // (whose literal after `=` is the password: `SET PASSWORD FOR u = 'x'`).
    let mut words = 0usize;
    let mut first_set = false;
    let mut set_password = false;
    let mut i = 0;
    let push = |prev: &mut [Tok; 2], t: Tok| {
        prev[1] = std::mem::replace(&mut prev[0], t);
    };
    while i < b.len() {
        let c = b[i];
        // Whitespace and comments.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-')) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
            continue;
        }
        if c == b'\'' || c == b'"' {
            let end = quoted_end(b, i);
            let word = |t: &Tok, w: &str| matches!(t, Tok::Word(x) if x == w);
            let ends_password = |t: &Tok| matches!(t, Tok::Word(x) if x.ends_with("PASSWORD"));
            let secret = cred > 0
                || word(&prev[0], "BY")
                || ends_password(&prev[0])
                || (matches!(prev[0], Tok::Punct(b'=' | b'(')) && ends_password(&prev[1]))
                || (set_password && matches!(prev[0], Tok::Punct(b'=')))
                || (identified
                    && (word(&prev[0], "AS")
                        || word(&prev[0], "USING")
                        || word(&prev[0], "REPLACE")));
            if secret {
                out.push_str(&text[last..i]);
                out.push_str(SECRET);
                last = end;
                changed = true;
            }
            push(&mut prev, Tok::Other);
            i = end;
            continue;
        }
        if c == b'`' {
            i = quoted_end(b, i);
            push(&mut prev, Tok::Other);
            continue;
        }
        if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            let w = text[start..i].to_ascii_uppercase();
            // A bare value inside CREDENTIALS (k = v): secret too.
            if cred > 0 && matches!(prev[0], Tok::Punct(b'=')) {
                out.push_str(&text[last..start]);
                out.push_str(SECRET);
                last = i;
                changed = true;
            }
            if w == "IDENTIFIED" {
                identified = true;
            }
            cred_next = w == "CREDENTIALS";
            match words {
                0 => first_set = w == "SET",
                1 => set_password = first_set && w == "PASSWORD",
                _ => {}
            }
            words += 1;
            push(&mut prev, Tok::Word(w));
            continue;
        }
        // Punctuation.
        match c {
            b'(' if cred > 0 => cred += 1,
            b'(' if cred_next => cred = 1,
            b')' if cred > 0 => cred -= 1,
            b';' => {
                identified = false;
                cred = 0;
                words = 0;
                first_set = false;
                set_password = false;
            }
            _ => {}
        }
        cred_next = false;
        push(&mut prev, Tok::Punct(c));
        // Keep char boundaries: step over the whole UTF-8 sequence.
        i += 1;
        while i < b.len() && (b[i] & 0xC0) == 0x80 {
            i += 1;
        }
    }
    if !changed {
        return Cow::Borrowed(text);
    }
    out.push_str(&text[last..]);
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::info;

    #[test]
    fn passwords_and_credentials_are_hidden() {
        for (sql, want) in [
            (
                "CREATE USER 'u'@'%' IDENTIFIED BY 's3cr3t'",
                "CREATE USER 'u'@'%' IDENTIFIED BY <secret>",
            ),
            (
                "alter user u identified with caching_sha2_password by \"p'w\"",
                "alter user u identified with caching_sha2_password by <secret>",
            ),
            (
                "ALTER USER u IDENTIFIED BY 'new' REPLACE 'old' RETAIN CURRENT PASSWORD",
                "ALTER USER u IDENTIFIED BY <secret> REPLACE <secret> RETAIN CURRENT PASSWORD",
            ),
            (
                "CREATE USER u IDENTIFIED WITH mysql_native_password AS '*ABC'",
                "CREATE USER u IDENTIFIED WITH mysql_native_password AS <secret>",
            ),
            (
                "CREATE USER u IDENTIFIED VIA ed25519 USING PASSWORD('pw')",
                "CREATE USER u IDENTIFIED VIA ed25519 USING PASSWORD(<secret>)",
            ),
            (
                "SET PASSWORD FOR u = 'x\\'y'",
                "SET PASSWORD FOR u = <secret>",
            ),
            (
                "SET PASSWORD = PASSWORD('x')",
                "SET PASSWORD = PASSWORD(<secret>)",
            ),
            (
                "CHANGE MASTER TO MASTER_PASSWORD = 'm'",
                "CHANGE MASTER TO MASTER_PASSWORD = <secret>",
            ),
            (
                "CREATE CONNECTION c TYPE webhook WITH (url = 'http://h/x') CREDENTIALS (token = 't', signing_secret = \"s\")",
                "CREATE CONNECTION c TYPE webhook WITH (url = 'http://h/x') CREDENTIALS (token = <secret>, signing_secret = <secret>)",
            ),
            (
                "alter connection c set credentials (password = 'p'); SELECT 'keep'",
                "alter connection c set credentials (password = <secret>); SELECT 'keep'",
            ),
            (
                "CREATE CONNECTION c TYPE s3 CREDENTIALS (secret_access_key = abc123)",
                "CREATE CONNECTION c TYPE s3 CREDENTIALS (secret_access_key = <secret>)",
            ),
            // Cut off at the 1,024-character limit inside the literal.
            (
                "CREATE USER u IDENTIFIED BY 'abc",
                "CREATE USER u IDENTIFIED BY <secret>",
            ),
        ] {
            assert_eq!(info(sql), want, "{sql}");
        }
    }

    #[test]
    fn other_text_is_untouched() {
        for sql in [
            "SELECT 'IDENTIFIED BY' FROM t WHERE password = 'x'",
            "SELECT * FROM users WHERE name = 'by'",
            "INSERT INTO t (a) VALUES ('é', 'credentials')",
            "SELECT 1 AS 'x' -- IDENTIFIED BY 'c'",
        ] {
            let got = info(sql);
            // A column named password compared with a literal is hidden
            // (as a PASSWORD = 'x' form): nothing else changes.
            if sql.contains("password = 'x'") {
                assert_eq!(
                    got,
                    "SELECT 'IDENTIFIED BY' FROM t WHERE password = <secret>"
                );
            } else {
                assert_eq!(got, sql);
            }
        }
    }
}
