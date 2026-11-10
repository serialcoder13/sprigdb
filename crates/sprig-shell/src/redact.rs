//! Redaction of secrets in statements (chapter 53, "Redaction").
//!
//! Applied before a statement goes into history or a log, or is shown in a
//! status line. Strings after the words that introduce secrets become
//! `'***'`, and words that look like API keys or session tokens become
//! `***`. A statement whose secrets can't be found reliably (an open
//! string) is kept only as a placeholder.

use crate::lang::{Lang, Tok, classify_sql, lex, open_at_end, redis_argv};

/// Words after which the next string literal is a secret.
const SECRET_WORDS: &[&str] = &[
    "BY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIALS",
    "API_KEY",
    "ACCESS_KEY",
    "SECRET_KEY",
    "SASL_PASSWORD",
    "PWD",
    "AUTH",
];

/// Prefixes of API keys and session tokens.
const TOKEN_PREFIXES: &[&str] = &["sprig_", "sprigat_", "sprigrt_"];

fn looks_like_token(w: &str) -> bool {
    let w = w.trim_matches(|c| c == '\'' || c == '"');
    TOKEN_PREFIXES
        .iter()
        .any(|p| w.starts_with(p) && w.len() > p.len() + 8)
}

/// The statement with its secrets masked.
#[must_use]
pub fn redact(stmt: &str, lang: Lang) -> String {
    match lang {
        Lang::Redis => redact_redis(stmt),
        Lang::Mongo => redact_mongo(stmt),
        Lang::Sql | Lang::Cypher => redact_sql(stmt, lang == Lang::Cypher),
    }
}

fn redact_sql(stmt: &str, cypher: bool) -> String {
    if open_at_end(stmt, cypher) {
        let class = format!("{:?}", classify_sql(stmt)).to_ascii_lowercase();
        return format!("-- redacted: {class} statement");
    }
    let toks = lex(stmt, cypher);
    let mut out = String::with_capacity(stmt.len());
    // Set by a secret word; the next string (after `=`, `:` or spaces) is
    // masked.
    let mut armed = false;
    for t in &toks {
        let text = t.text(stmt);
        match t.kind {
            Tok::Word => {
                let up = text.to_ascii_uppercase();
                if looks_like_token(text) {
                    out.push_str("***");
                    armed = false;
                    continue;
                }
                armed = SECRET_WORDS.contains(&up.as_str())
                    || (armed && matches!(up.as_str(), "USING" | "WITH"));
                out.push_str(text);
            }
            Tok::Str => {
                if armed || looks_like_token(text) {
                    out.push_str("'***'");
                } else {
                    out.push_str(text);
                }
                armed = false;
            }
            Tok::Op if matches!(text, "=" | ":" | "(") => out.push_str(text),
            Tok::Space | Tok::Comment => out.push_str(text),
            _ => {
                armed = false;
                out.push_str(text);
            }
        }
    }
    out
}

fn redact_redis(line: &str) -> String {
    let Ok(argv) = redis_argv(line) else {
        return "# redacted: redis command".to_owned();
    };
    let Some(cmd) = argv.first().map(|c| c.to_ascii_uppercase()) else {
        return String::new();
    };
    let mut out: Vec<String> = Vec::with_capacity(argv.len());
    let mut mask_from = usize::MAX;
    if cmd == "AUTH" {
        mask_from = 1;
    }
    for (i, a) in argv.iter().enumerate() {
        let up = a.to_ascii_uppercase();
        if i >= mask_from || looks_like_token(a) {
            out.push("***".to_owned());
            continue;
        }
        // HELLO 3 AUTH user pass; MIGRATE ... AUTH pass / AUTH2 user pass:
        // everything after the word.
        if i > 0 && (up == "AUTH" || up == "AUTH2") && cmd != "AUTH" {
            out.push(a.clone());
            mask_from = i + 1;
            continue;
        }
        if a.chars().any(char::is_whitespace) || a.is_empty() {
            out.push(format!("{a:?}"));
        } else {
            out.push(a.clone());
        }
    }
    out.join(" ")
}

fn redact_mongo(stmt: &str) -> String {
    // Fields named like secrets: `pwd: "..."`, `password: '...'`.
    let c: Vec<char> = stmt.chars().collect();
    let rest_starts_colon = |from: usize| {
        c[from..]
            .iter()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|&ch| ch == ':')
    };
    let mut out = String::with_capacity(stmt.len());
    let mut last_key = String::new();
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch == '"' || ch == '\'' {
            let start = i;
            i += 1;
            while i < c.len() && c[i] != ch {
                if c[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            i = (i + 1).min(c.len());
            let lit: String = c[start..i].iter().collect();
            if rest_starts_colon(i) {
                last_key = lit
                    .trim_matches(|q| q == '"' || q == '\'')
                    .to_ascii_lowercase();
                out.push_str(&lit);
            } else if is_secret_key(&last_key) || looks_like_token(&lit) {
                out.push_str("\"***\"");
                last_key.clear();
            } else {
                out.push_str(&lit);
                last_key.clear();
            }
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            while i < c.len() && (c[i].is_ascii_alphanumeric() || c[i] == '_') {
                i += 1;
            }
            let w: String = c[start..i].iter().collect();
            if rest_starts_colon(i) {
                last_key = w.to_ascii_lowercase();
            }
            out.push_str(&w);
            continue;
        }
        if matches!(ch, ',' | '{' | '}') {
            last_key.clear();
        }
        out.push(ch);
        i += 1;
    }
    out
}

fn is_secret_key(k: &str) -> bool {
    matches!(
        k,
        "pwd"
            | "password"
            | "secret"
            | "token"
            | "api_key"
            | "apikey"
            | "access_key"
            | "secret_key"
            | "credentials"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_secrets() {
        let r = |s| redact(s, Lang::Sql);
        assert_eq!(
            r("CREATE USER bob IDENTIFIED BY 's3cret'"),
            "CREATE USER bob IDENTIFIED BY '***'"
        );
        assert_eq!(
            r("ALTER USER bob IDENTIFIED WITH caching_sha2_password BY \"pw\""),
            "ALTER USER bob IDENTIFIED WITH caching_sha2_password BY '***'"
        );
        assert_eq!(r("SET PASSWORD = 'x'"), "SET PASSWORD = '***'");
        assert_eq!(
            r(
                "CREATE CONNECTION s3 TYPE s3 OPTIONS (access_key = 'AK', secret_key = 'SK', region = 'eu')"
            ),
            "CREATE CONNECTION s3 TYPE s3 OPTIONS (access_key = '***', secret_key = '***', region = 'eu')"
        );
        assert_eq!(
            r("SELECT 'sprig_0123456789abcdef' AS k, 'by the way'"),
            "SELECT '***' AS k, 'by the way'"
        );
        assert_eq!(
            r("SELECT * FROM t ORDER BY name"),
            "SELECT * FROM t ORDER BY name"
        );
        assert_eq!(r("SELECT 'open"), "-- redacted: read statement");
    }

    #[test]
    fn redis_and_mongo_secrets() {
        assert_eq!(redact("AUTH alice hunter2", Lang::Redis), "AUTH *** ***");
        assert_eq!(
            redact("HELLO 3 AUTH alice hunter2", Lang::Redis),
            "HELLO 3 AUTH *** ***"
        );
        assert_eq!(redact("SET k \"a b\"", Lang::Redis), "SET k \"a b\"");
        assert_eq!(
            redact(
                "db.createUser({user: 'x', pwd: 'hunter2', roles: []})",
                Lang::Mongo
            ),
            "db.createUser({user: 'x', pwd: \"***\", roles: []})"
        );
        assert_eq!(
            redact("db.t.find({name: 'é', \"password\": \"p\"})", Lang::Mongo),
            "db.t.find({name: 'é', \"password\": \"***\"})"
        );
    }
}
