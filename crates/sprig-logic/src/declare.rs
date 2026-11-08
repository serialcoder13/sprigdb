//! The declaration surface (chapter 41, "Declaring code"; build step 3,
//! functions and procedures):
//!
//! ```text
//! CREATE [OR REPLACE] {FUNCTION | PROCEDURE} [db.]name [(params)] [RETURNS type]
//!   LANGUAGE {kirun | javascript}
//!   [SQL SECURITY {INVOKER | DEFINER}] [[NOT] DETERMINISTIC]
//!   [EXPOSE (sql, http, ...)] [WITH (SPRIG_TIER = 'auto' | 'mariadb' | 'engine')]
//!   [COMMENT 'text']
//! AS {'text' | "text" | $$ text $$}
//! ```
//!
//! Clauses may come in any order before `AS`. A KIRun procedure's
//! signature comes from its definition, so its parameter list is ignored.
//! `LANGUAGE sql` (or no `LANGUAGE`) is ordinary DDL and not parsed here.

use serde::Serialize;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::Runtime;

/// `FUNCTION` or `PROCEDURE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// `CREATE FUNCTION`.
    Function,
    /// `CREATE PROCEDURE`.
    Procedure,
}

impl Kind {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Kind::Function => "function",
            Kind::Procedure => "procedure",
        }
    }
}

/// `SPRIG_TIER`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TierPref {
    /// Placement decides.
    Auto,
    /// MariaDB or refuse (39868).
    Mariadb,
    /// Always the engine tier.
    Engine,
}

impl TierPref {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TierPref::Auto => "auto",
            TierPref::Mariadb => "mariadb",
            TierPref::Engine => "engine",
        }
    }
}

/// A parsed declaration.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Declaration {
    /// Function or procedure.
    pub kind: Kind,
    /// `OR REPLACE` was given.
    pub or_replace: bool,
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// The language.
    pub runtime: Runtime,
    /// `SQL SECURITY DEFINER`.
    pub definer_security: bool,
    /// `DETERMINISTIC`.
    pub deterministic: bool,
    /// `EXPOSE (...)`, lower case; `["sql"]` by default.
    pub exposure: Vec<String>,
    /// `SPRIG_TIER`.
    pub tier: TierPref,
    /// The body.
    pub body: String,
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSyntax, m)
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    /// A quoted name.
    Name(String),
    Str(String),
    Punct(char),
    Body(String),
}

fn lex(src: &str) -> Result<Vec<Tok>> {
    let mut out = Vec::new();
    let cs: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '-' && cs.get(i + 1) == Some(&'-') {
            while i < cs.len() && cs[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && cs.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < cs.len() && !(cs[i] == '*' && cs[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        if c == '$' && cs.get(i + 1) == Some(&'$') {
            let start = i + 2;
            let mut j = start;
            while j + 1 < cs.len() && !(cs[j] == '$' && cs[j + 1] == '$') {
                j += 1;
            }
            if j + 1 >= cs.len() {
                return Err(syntax("unterminated $$ body"));
            }
            out.push(Tok::Body(cs[start..j].iter().collect()));
            i = j + 2;
            continue;
        }
        if c == '\'' || c == '"' || c == '`' {
            let q = c;
            let mut s = String::new();
            i += 1;
            loop {
                let Some(&ch) = cs.get(i) else {
                    return Err(syntax("unterminated quoted text"));
                };
                i += 1;
                if ch == q {
                    if cs.get(i) == Some(&q) {
                        s.push(q);
                        i += 1;
                        continue;
                    }
                    break;
                }
                if ch == '\\' && q != '`' {
                    let Some(&e) = cs.get(i) else {
                        return Err(syntax("unterminated quoted text"));
                    };
                    i += 1;
                    s.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '0' => '\0',
                        other => other,
                    });
                    continue;
                }
                s.push(ch);
            }
            out.push(if q == '`' { Tok::Name(s) } else { Tok::Str(s) });
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < cs.len() && (cs[i].is_alphanumeric() || cs[i] == '_' || cs[i] == '$') {
                i += 1;
            }
            out.push(Tok::Word(cs[start..i].iter().collect()));
            continue;
        }
        out.push(Tok::Punct(c));
        i += 1;
    }
    Ok(out)
}

fn is_kw(t: Option<&Tok>, kw: &str) -> bool {
    matches!(t, Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
}

/// Whether `sql` is a logic declaration (`CREATE FUNCTION|PROCEDURE ...
/// LANGUAGE kirun|javascript`), cheaply, without full parsing.
#[must_use]
pub fn is_declaration(sql: &str) -> bool {
    let head: String = sql
        .chars()
        .take(4096)
        .collect::<String>()
        .to_ascii_lowercase();
    let t = head.trim_start();
    if !t.starts_with("create") {
        return false;
    }
    if !(t.contains("function") || t.contains("procedure")) {
        return false;
    }
    let words: Vec<&str> = t
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .collect();
    words
        .windows(2)
        .any(|w| w[0] == "language" && matches!(w[1], "kirun" | "javascript" | "js"))
}

/// Parses a declaration.
///
/// # Errors
/// 39861 for text that is not a declaration of this form.
pub fn parse(sql: &str) -> Result<Declaration> {
    let toks = lex(sql.trim().trim_end_matches(';'))?;
    let mut i = 0;
    let next = |i: &mut usize| -> Option<Tok> {
        let t = toks.get(*i).cloned();
        *i += 1;
        t
    };
    if !is_kw(toks.get(i), "create") {
        return Err(syntax("a declaration starts with CREATE"));
    }
    i += 1;
    let mut or_replace = false;
    if is_kw(toks.get(i), "or") && is_kw(toks.get(i + 1), "replace") {
        or_replace = true;
        i += 2;
    }
    let kind = match next(&mut i) {
        Some(Tok::Word(w)) if w.eq_ignore_ascii_case("function") => Kind::Function,
        Some(Tok::Word(w)) if w.eq_ignore_ascii_case("procedure") => Kind::Procedure,
        _ => {
            return Err(syntax(
                "expected FUNCTION or PROCEDURE (DEFINER= isn't allowed here)",
            ));
        }
    };
    let ident = |t: Option<Tok>| match t {
        Some(Tok::Word(w) | Tok::Name(w)) => Ok(w),
        _ => Err(syntax("expected a name")),
    };
    let first = ident(next(&mut i))?;
    let (db, name) = if toks.get(i) == Some(&Tok::Punct('.')) {
        i += 1;
        (Some(first), ident(next(&mut i))?)
    } else {
        (None, first)
    };
    if name.is_empty() || name.chars().count() > 64 {
        return Err(syntax("a routine name has 1 to 64 characters"));
    }
    // A parameter list, skipped (balanced parentheses).
    if toks.get(i) == Some(&Tok::Punct('(')) {
        let mut depth = 0;
        loop {
            match next(&mut i) {
                Some(Tok::Punct('(')) => depth += 1,
                Some(Tok::Punct(')')) => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                Some(_) => {}
                None => return Err(syntax("unbalanced parameter list")),
            }
        }
    }
    let mut d = Declaration {
        kind,
        or_replace,
        db,
        name,
        runtime: Runtime::Sql,
        definer_security: false,
        deterministic: false,
        exposure: vec!["sql".to_owned()],
        tier: TierPref::Auto,
        body: String::new(),
    };
    let mut language = None;
    loop {
        let Some(t) = next(&mut i) else {
            return Err(syntax("expected AS and a body"));
        };
        let Tok::Word(w) = t else {
            return Err(syntax(format!("unexpected {t:?}")));
        };
        match w.to_ascii_lowercase().as_str() {
            "returns" => {
                // A type: a word and an optional (n[, m]).
                next(&mut i);
                if toks.get(i) == Some(&Tok::Punct('(')) {
                    while !matches!(next(&mut i), Some(Tok::Punct(')')) | None) {}
                }
            }
            "language" => {
                let l = ident(next(&mut i))?;
                language = Some(
                    sprig_logic_compile::Runtime::parse(&l)
                        .ok_or_else(|| syntax(format!("unknown LANGUAGE {l}")))?,
                );
            }
            "sql" => {
                if !is_kw(toks.get(i), "security") {
                    return Err(syntax("expected SQL SECURITY"));
                }
                i += 1;
                let v = ident(next(&mut i))?.to_ascii_lowercase();
                d.definer_security = match v.as_str() {
                    "definer" => true,
                    "invoker" => false,
                    _ => return Err(syntax("SQL SECURITY takes INVOKER or DEFINER")),
                };
            }
            "deterministic" => d.deterministic = true,
            "not" => {
                if !is_kw(next(&mut i).as_ref(), "deterministic") {
                    return Err(syntax("expected NOT DETERMINISTIC"));
                }
            }
            "comment" => {
                if !matches!(next(&mut i), Some(Tok::Str(_))) {
                    return Err(syntax("COMMENT takes a string"));
                }
            }
            "expose" => {
                if next(&mut i) != Some(Tok::Punct('(')) {
                    return Err(syntax("EXPOSE takes a list"));
                }
                let mut list = Vec::new();
                loop {
                    match next(&mut i) {
                        Some(Tok::Word(x)) => {
                            let x = x.to_ascii_lowercase();
                            if !matches!(x.as_str(), "sql" | "http" | "grpc" | "mongo" | "redis") {
                                return Err(syntax(format!("EXPOSE: unknown protocol {x}")));
                            }
                            list.push(x);
                        }
                        Some(Tok::Punct(',')) => {}
                        Some(Tok::Punct(')')) => break,
                        _ => return Err(syntax("EXPOSE: bad list")),
                    }
                }
                d.exposure = list;
            }
            "with" => {
                if next(&mut i) != Some(Tok::Punct('('))
                    || !is_kw(next(&mut i).as_ref(), "sprig_tier")
                {
                    return Err(syntax("expected WITH (SPRIG_TIER = ...)"));
                }
                if next(&mut i) != Some(Tok::Punct('=')) {
                    return Err(syntax("expected SPRIG_TIER = '...'"));
                }
                d.tier = match next(&mut i) {
                    Some(Tok::Str(v) | Tok::Word(v)) => match v.to_ascii_lowercase().as_str() {
                        "auto" => TierPref::Auto,
                        "mariadb" => TierPref::Mariadb,
                        "engine" => TierPref::Engine,
                        _ => return Err(syntax("SPRIG_TIER is 'auto', 'mariadb' or 'engine'")),
                    },
                    _ => return Err(syntax("SPRIG_TIER takes a string")),
                };
                if next(&mut i) != Some(Tok::Punct(')')) {
                    return Err(syntax("expected )"));
                }
            }
            "as" => {
                d.body = match next(&mut i) {
                    Some(Tok::Str(b) | Tok::Body(b)) => b,
                    _ => return Err(syntax("AS takes a quoted or $$ body")),
                };
                if i < toks.len() {
                    return Err(syntax("text after the body"));
                }
                break;
            }
            other => return Err(syntax(format!("unexpected {other}"))),
        }
    }
    d.runtime = language.ok_or_else(|| syntax("LANGUAGE is missing"))?;
    Ok(d)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn kirun_procedure() {
        let s = "CREATE OR REPLACE PROCEDURE shop.quote LANGUAGE kirun SQL SECURITY DEFINER \
                 WITH (SPRIG_TIER = 'mariadb') EXPOSE (sql, http) AS '{\"name\": \"Q\", \"x\": \"it''s\"}';";
        assert!(is_declaration(s));
        let d = parse(s).unwrap();
        assert_eq!(d.kind, Kind::Procedure);
        assert!(d.or_replace);
        assert_eq!(d.db.as_deref(), Some("shop"));
        assert_eq!(d.name, "quote");
        assert_eq!(d.runtime, Runtime::Kirun);
        assert!(d.definer_security);
        assert_eq!(d.tier, TierPref::Mariadb);
        assert_eq!(d.exposure, vec!["sql", "http"]);
        assert_eq!(d.body, "{\"name\": \"Q\", \"x\": \"it's\"}");
    }

    #[test]
    fn javascript_function_with_dollar_body() {
        let s = "create function `order total`(id BIGINT) returns decimal(12, 2) language javascript \
                 deterministic as $$ export default (id) => id * 2; $$";
        let d = parse(s).unwrap();
        assert_eq!(d.kind, Kind::Function);
        assert_eq!(d.name, "order total");
        assert_eq!(d.runtime, Runtime::Javascript);
        assert!(d.deterministic);
        assert_eq!(d.body.trim(), "export default (id) => id * 2;");
    }

    #[test]
    fn ordinary_ddl_is_not_a_declaration() {
        assert!(!is_declaration("CREATE PROCEDURE p() BEGIN SELECT 1; END"));
        assert!(!is_declaration(
            "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL RETURN 1"
        ));
        assert!(!is_declaration("SELECT 'language kirun'"));
    }

    #[test]
    fn errors() {
        for s in [
            "CREATE PROCEDURE p LANGUAGE kirun",
            "CREATE PROCEDURE p LANGUAGE cobol AS '1'",
            "CREATE PROCEDURE p AS '1'",
            "CREATE PROCEDURE p LANGUAGE kirun WITH (SPRIG_TIER = 'gpu') AS '1'",
            "CREATE PROCEDURE p LANGUAGE kirun AS '1' extra",
            "CREATE DEFINER=x PROCEDURE p LANGUAGE kirun AS '1'",
        ] {
            assert_eq!(
                parse(s).unwrap_err().code(),
                Some(SprigCode::LogicSyntax),
                "{s}"
            );
        }
    }
}
