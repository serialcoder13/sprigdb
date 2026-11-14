//! Measure expressions and filters: SQL over the fact row.
//!
//! A model's expressions reach MariaDB inside the compiled SQL, so they
//! are restricted to a small, safe grammar: column names, numbers, quoted
//! strings, arithmetic and comparison operators, parentheses, commas and
//! the words `AND OR NOT IN IS NULL BETWEEN LIKE CASE WHEN THEN ELSE END
//! TRUE FALSE`. Column names are qualified with the fact's alias when the
//! SQL is built. No functions, subqueries, comments or semicolons.

use std::fmt::Write as _;

const WORDS: &[&str] = &[
    "AND", "OR", "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE",
    "END", "TRUE", "FALSE", "DIV", "MOD",
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Word(String),
    Num(String),
    Str(String),
    Op(String),
}

fn lex(s: &str) -> Result<Vec<Tok>, String> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let st = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let w = &s[st..i];
            if WORDS.contains(&w.to_ascii_uppercase().as_str()) {
                out.push(Tok::Word(w.to_ascii_uppercase()));
            } else {
                out.push(Tok::Ident(w.to_owned()));
            }
        } else if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            let st = i;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                i += 1;
                if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                    i += 1;
                }
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            out.push(Tok::Num(s[st..i].to_owned()));
        } else if c == b'\'' {
            let mut v = String::new();
            i += 1;
            loop {
                match b.get(i) {
                    None => return Err("an unterminated string".into()),
                    Some(b'\'') if b.get(i + 1) == Some(&b'\'') => {
                        v.push('\'');
                        i += 2;
                    }
                    Some(b'\'') => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') => return Err("backslashes aren't allowed in strings".into()),
                    Some(_) => {
                        let ch = s[i..].chars().next().unwrap_or(' ');
                        v.push(ch);
                        i += ch.len_utf8();
                    }
                }
            }
            out.push(Tok::Str(v));
        } else {
            let two = s.get(i..i + 2).unwrap_or("");
            if ["<=", ">=", "<>", "!="].contains(&two) {
                out.push(Tok::Op(two.to_owned()));
                i += 2;
            } else if b"+-*/%()=<>,".contains(&c) {
                out.push(Tok::Op((c as char).to_string()));
                i += 1;
            } else {
                return Err(format!("'{}' isn't allowed", c as char));
            }
        }
    }
    Ok(out)
}

/// Checks an expression; the reason when refused.
///
/// # Errors
/// Empty text, unknown characters, unbalanced parentheses.
pub fn check(s: &str) -> Result<(), String> {
    let toks = lex(s)?;
    if toks.is_empty() {
        return Err("empty expression".into());
    }
    let mut depth = 0i32;
    for t in &toks {
        match t {
            Tok::Op(o) if o == "(" => depth += 1,
            Tok::Op(o) if o == ")" => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced parentheses".into());
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("unbalanced parentheses".into());
    }
    // An identifier followed by "(" would be a function call.
    for w in toks.windows(2) {
        if matches!(&w[0], Tok::Ident(_)) && w[1] == Tok::Op("(".into()) {
            return Err("functions aren't allowed".into());
        }
    }
    Ok(())
}

/// The columns an expression names.
#[must_use]
pub fn columns(s: &str) -> Vec<String> {
    lex(s)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| match t {
            Tok::Ident(i) => Some(i),
            _ => None,
        })
        .collect()
}

/// The expression as SQL, its columns qualified with `alias`.
#[must_use]
pub fn qualify(s: &str, alias: &str) -> String {
    let mut out = String::new();
    for t in lex(s).unwrap_or_default() {
        if !out.is_empty() {
            out.push(' ');
        }
        match t {
            Tok::Ident(i) => {
                let _ = write!(out, "{alias}.`{i}`");
            }
            Tok::Word(w) | Tok::Num(w) | Tok::Op(w) => out.push_str(&w),
            Tok::Str(v) => {
                let _ = write!(out, "'{}'", v.replace('\'', "''"));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expressions() {
        assert!(check("amount").is_ok());
        assert!(check("qty * price - discount").is_ok());
        assert!(check("status = 'paid' AND qty > 0").is_ok());
        assert!(check("CASE WHEN qty > 1 THEN 1 ELSE 0 END").is_ok());
        assert!(check("").is_err());
        assert!(check("sleep(10)").is_err());
        assert!(check("a; DROP TABLE x").is_err());
        // Tokens are printed apart, so "--" never becomes a comment.
        assert_eq!(qualify("a -- x", "f"), "f.`a` - - f.`x`");
        assert!(check("(a").is_err());
        assert!(check("'x\\'").is_err());
        assert_eq!(
            qualify("status = 'it''s' AND qty>0", "f"),
            "f.`status` = 'it''s' AND f.`qty` > 0"
        );
        assert_eq!(columns("qty * price + 1"), vec!["qty", "price"]);
    }
}
