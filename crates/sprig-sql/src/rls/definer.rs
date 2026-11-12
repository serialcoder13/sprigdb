//! Definer's policies for `SQL SECURITY DEFINER` routines (chapter 29;
//! RLS round 6, decision RL57).
//!
//! A definer routine over protected tables is pointed at the policy views
//! (RL20) and wrapped so that, while it runs, MariaDB's policy views and
//! write triggers apply its definer's policies instead of the caller's.
//! The gateway puts every wrapped routine's definer context in the
//! session's own context, under `"d"` (account → context, computed at the
//! current auth version for every statement), so a role or policy change
//! of the definer applies at once and nothing is redeployed:
//!
//! ```text
//! BEGIN
//!   DECLARE saved = @sprig_rls
//!   EXIT HANDLER FOR SQLEXCEPTION: restore, RESIGNAL
//!   CONTINUE HANDLER FOR NOT FOUND: a SIGNAL's (errno <> 1329): restore, RESIGNAL
//!   @sprig_rls = d[<definer>] with the same "d", or the caller's when absent
//!   <the body as written>          (functions: every RETURN e restores first)
//!   restore
//! END
//! ```
//!
//! The caller's context comes back on every exit: the body's end, an
//! error (the handler re-raises it), a `SIGNAL` of class `02` (re-raised
//! too), and `RETURN` (the value is computed under the definer's context,
//! then the caller's is restored). A routine without a `"d"` entry for its
//! definer (worker-run code, RL42) keeps the caller's context: never
//! wider than before. [`unwrap`] gives the text as written back, for
//! `SHOW CREATE`.

#![allow(clippy::many_single_char_names)] // token indexes: t, n, k, m, i, j

use std::borrow::Cow;

use sprig_common::SprigError;

use super::text::Toks;
use crate::lex::{self, LexOptions, TokenKind};

/// The saved caller's context.
const SAVED: &str = "`__sprig_rls_saved`";
/// A function's result, computed under the definer's context.
const RESULT: &str = "`__sprig_rls_r`";
/// The body's start and end marks.
const BODY_OPEN: &str = "/*sprig:rls-definer(*/";
const BODY_CLOSE: &str = "/*)sprig:rls-definer*/";
/// A rewritten `RETURN`'s marks; the original keyword (and the space after
/// it) sits in the opening comment.
const RET_OPEN: &str = "/*sprig:rr(";
const RET_CLOSE: &str = "/*)sprig:rr*/";

/// A wrapped routine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wrapped {
    /// The `CREATE` text to run.
    pub text: String,
    /// The definer's account (`t42_u7`).
    pub account: String,
}

/// Whether `text` holds a wrapped body.
#[must_use]
pub fn is_wrapped(text: &str) -> bool {
    text.contains(BODY_OPEN)
}

fn unsupported(why: &str) -> SprigError {
    super::unsupported(&format!("a SQL SECURITY DEFINER routine ({why})"))
}

/// Words that start a routine characteristic.
const CHARACTERISTICS: &[&str] = &[
    "COMMENT",
    "LANGUAGE",
    "NOT",
    "DETERMINISTIC",
    "CONTAINS",
    "NO",
    "READS",
    "MODIFIES",
    "SQL",
];

/// The wrapper's head for definer `account`.
fn head(account: &str) -> String {
    let path = format!("$.d.\"{}\"", account.replace(['"', '\\', '\''], ""));
    format!(
        "BEGIN DECLARE {SAVED} LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT @sprig_rls; \
         DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET @sprig_rls = {SAVED}; RESIGNAL; END; \
         DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN DECLARE `__sprig_rls_e` INT DEFAULT 0; \
         GET DIAGNOSTICS CONDITION 1 `__sprig_rls_e` = MYSQL_ERRNO; IF `__sprig_rls_e` <> 1329 \
         THEN SET @sprig_rls = {SAVED}; RESIGNAL; END IF; END; \
         SET @sprig_rls = COALESCE(JSON_SET(JSON_EXTRACT(@sprig_rls, '{path}'), '$.d', \
         JSON_EXTRACT(@sprig_rls, '$.d')), @sprig_rls); {BODY_OPEN}"
    )
}

/// The wrapper's tail.
fn tail() -> String {
    format!("{BODY_CLOSE}; SET @sprig_rls = {SAVED}; END")
}

/// A rewritten `RETURN`'s head after its mark (`<kw>*/BEGIN ... = `).
fn ret_head(ty: &str) -> String {
    format!("*/BEGIN DECLARE {RESULT} {ty}; SET {RESULT} = ")
}

/// A rewritten `RETURN`'s tail.
fn ret_tail() -> String {
    format!("; SET @sprig_rls = {SAVED}; RETURN {RESULT}; END{RET_CLOSE}")
}

/// `text` (`SHOW CREATE PROCEDURE` or `FUNCTION`'s) wrapped (see the
/// module docs); `Ok(None)` for an invoker routine or another statement.
/// An already wrapped text is unwrapped first.
///
/// # Errors
/// 39366 for a text whose signature or body can't be found.
#[allow(clippy::too_many_lines)] // one pass over the signature
pub fn wrap(text: &str, lex_opts: &LexOptions) -> Result<Option<Wrapped>, SprigError> {
    let base = unwrap(text);
    let text = base.as_ref();
    let all = lex::lex(text.as_bytes(), lex_opts).map_err(|_| unsupported("too long"))?;
    let t = Toks::new(text.as_bytes(), &all);
    let n = t.toks.len();
    let Some(kw) = (0..n).find(|i| t.word(*i, "PROCEDURE") || t.word(*i, "FUNCTION")) else {
        return Ok(None);
    };
    if !t.word(0, "CREATE") {
        return Ok(None);
    }
    let function = t.word(kw, "FUNCTION");
    // DEFINER=`t42_u7`@`%`, as SHOW CREATE prints it.
    let head_text = &text[..t.start(kw)];
    let account = head_text
        .find("DEFINER=`")
        .map(|at| &head_text[at + 9..])
        .and_then(|r| r.split('`').next())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| unsupported("no definer"))?
        .to_owned();
    // The name, then the parameter list.
    let mut k = kw + 1;
    if t.punct(k + 1, ".") {
        k += 2;
    }
    k += 1;
    if !t.punct(k, "(") {
        return Err(unsupported("no parameter list"));
    }
    k = t.matching(k).ok_or_else(|| unsupported("parameters"))? + 1;
    // RETURNS <type> for functions: up to a characteristic or the body.
    let mut rtype = None;
    if function {
        if !t.word(k, "RETURNS") {
            return Err(unsupported("no RETURNS"));
        }
        let from = k + 1;
        let mut m = from;
        while m < n {
            let w = word(&t, m);
            let label = t.toks[m].kind.is_ident_like() && t.punct(m + 1, ":");
            if CHARACTERISTICS.contains(&w.as_str())
                || matches!(w.as_str(), "BEGIN" | "RETURN")
                || label
            {
                break;
            }
            if t.punct(m, "(") {
                m = t.matching(m).ok_or_else(|| unsupported("return type"))?;
            }
            m += 1;
        }
        if m == from || m >= n {
            return Err(unsupported("return type"));
        }
        rtype = Some(text[t.start(from)..t.end(m - 1)].to_owned());
        k = m;
    }
    // Characteristics.
    let mut invoker = false;
    loop {
        match word(&t, k).as_str() {
            "COMMENT" | "LANGUAGE" | "CONTAINS" | "NO" | "NOT" => k += 2,
            "DETERMINISTIC" => k += 1,
            "READS" | "MODIFIES" => k += 3,
            "SQL" if t.word(k + 1, "SECURITY") => {
                invoker = t.word(k + 2, "INVOKER");
                k += 3;
            }
            _ => break,
        }
    }
    if invoker {
        return Ok(None);
    }
    if k >= n {
        return Err(unsupported("no body"));
    }
    let body_start = t.start(k);
    let mut body_end = text.trim_end().len();
    if text[..body_end].ends_with(';') {
        body_end -= 1;
    }
    // Functions: every RETURN restores the caller's context first.
    let mut body = String::new();
    let mut at = body_start;
    if let Some(ty) = &rtype {
        let mut i = k;
        while i < n {
            if t.word(i, "RETURN") && t.toks[i].kind == TokenKind::Word {
                // The expression: to the next `;` at this depth, or the end.
                let mut j = i + 1;
                let mut last = None;
                while j < n && !t.punct(j, ";") {
                    if t.punct(j, "(") {
                        j = t.matching(j).ok_or_else(|| unsupported("RETURN"))?;
                    }
                    last = Some(j);
                    j += 1;
                }
                let Some(last) = last else {
                    return Err(unsupported("an empty RETURN"));
                };
                let kw_text = &text[t.start(i)..t.start(i + 1)];
                if kw_text.contains("*/") {
                    return Err(unsupported("RETURN"));
                }
                body.push_str(&text[at..t.start(i)]);
                body.push_str(RET_OPEN);
                body.push_str(kw_text);
                body.push_str(&ret_head(ty));
                body.push_str(&text[t.start(i + 1)..t.end(last)]);
                body.push_str(&ret_tail());
                at = t.end(last);
                i = j;
                continue;
            }
            i += 1;
        }
    }
    body.push_str(&text[at..body_end.max(at)]);
    let out = format!("{}{}{body}{}", &text[..body_start], head(&account), tail());
    Ok(Some(Wrapped { text: out, account }))
}

fn word(t: &Toks<'_>, i: usize) -> String {
    t.toks
        .get(i)
        .filter(|k| k.kind == TokenKind::Word)
        .map(|k| String::from_utf8_lossy(k.bytes(t.src)).to_ascii_uppercase())
        .unwrap_or_default()
}

/// A routine's text as written: the wrapper and the rewritten `RETURN`s
/// taken out (borrowed when there are none).
#[must_use]
pub fn unwrap(text: &str) -> Cow<'_, str> {
    let (Some(open), Some(close)) = (text.find(BODY_OPEN), text.rfind(BODY_CLOSE)) else {
        return Cow::Borrowed(text);
    };
    // The head starts at the wrapper's BEGIN, right after the signature.
    let Some(begin) = text[..open].rfind("BEGIN DECLARE `__sprig_rls_saved`") else {
        return Cow::Borrowed(text);
    };
    let mut body = &text[open + BODY_OPEN.len()..close];
    let mut out = String::with_capacity(text.len());
    out.push_str(&text[..begin]);
    while let Some(at) = body.find(RET_OPEN) {
        out.push_str(&body[..at]);
        let rest = &body[at + RET_OPEN.len()..];
        let Some(kw_end) = rest.find("*/BEGIN DECLARE ") else {
            break;
        };
        out.push_str(&rest[..kw_end]);
        let after = &rest[kw_end..];
        let Some(expr_at) = after.find(&format!("SET {RESULT} = ")) else {
            break;
        };
        let expr = &after[expr_at + RESULT.len() + 7..];
        let Some(end) = expr.find(&ret_tail()) else {
            break;
        };
        out.push_str(&expr[..end]);
        body = &expr[end + ret_tail().len()..];
    }
    out.push_str(body);
    let after = &text[close + BODY_CLOSE.len()..];
    let rest = after
        .strip_prefix(&tail()[BODY_CLOSE.len()..])
        .unwrap_or(after);
    out.push_str(rest);
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn procedures_switch_to_the_definer_and_back() {
        let p = "CREATE DEFINER=`t42_u7`@`%` PROCEDURE `p`(IN a INT)\n    MODIFIES SQL DATA\n    COMMENT 'x;y'\nBEGIN SELECT COUNT(*) FROM `__sprig_rls_docs` AS `docs`; END";
        let w = wrap(p, &LexOptions::mariadb()).unwrap().unwrap();
        assert_eq!(w.account, "t42_u7");
        assert!(
            w.text.starts_with(
                "CREATE DEFINER=`t42_u7`@`%` PROCEDURE `p`(IN a INT)\n    MODIFIES SQL DATA\n    COMMENT 'x;y'\nBEGIN DECLARE `__sprig_rls_saved` LONGTEXT"
            ),
            "{}",
            w.text
        );
        assert!(
            w.text
                .contains("JSON_EXTRACT(@sprig_rls, '$.d.\"t42_u7\"')")
        );
        assert!(w.text.ends_with(
            "/*sprig:rls-definer(*/BEGIN SELECT COUNT(*) FROM `__sprig_rls_docs` AS `docs`; END/*)sprig:rls-definer*/; SET @sprig_rls = `__sprig_rls_saved`; END"
        ));
        assert_eq!(unwrap(&w.text), p);
        // Wrapping again gives the same text.
        assert_eq!(wrap(&w.text, &LexOptions::mariadb()).unwrap().unwrap(), w);
        // Invoker routines aren't wrapped.
        let i = "CREATE DEFINER=`t42_u7`@`%` PROCEDURE `p`() SQL SECURITY INVOKER SELECT 1";
        assert_eq!(wrap(i, &LexOptions::mariadb()).unwrap(), None);
        assert_eq!(unwrap(i), i);
    }

    #[test]
    fn functions_restore_before_every_return() {
        let f = "CREATE DEFINER=`t42_u7`@`%` FUNCTION `f`(x INT) RETURNS varchar(10) CHARSET utf8mb4 COLLATE utf8mb4_bin\n    READS SQL DATA\nBEGIN IF x > 0 THEN RETURN (SELECT MAX(b) FROM t WHERE id = x); END IF; return  'none'; END";
        let w = wrap(f, &LexOptions::mariadb()).unwrap().unwrap();
        assert!(
            w.text.contains(
                "THEN /*sprig:rr(RETURN */BEGIN DECLARE `__sprig_rls_r` varchar(10) CHARSET utf8mb4 COLLATE utf8mb4_bin; SET `__sprig_rls_r` = (SELECT MAX(b) FROM t WHERE id = x); SET @sprig_rls = `__sprig_rls_saved`; RETURN `__sprig_rls_r`; END/*)sprig:rr*/; END IF; /*sprig:rr(return  */BEGIN"
            ),
            "{}",
            w.text
        );
        assert_eq!(unwrap(&w.text), f);
        // A single-statement body.
        let g = "CREATE DEFINER=`t42_u9`@`%` FUNCTION `g`() RETURNS int(11) DETERMINISTIC RETURN (SELECT COUNT(*) FROM t)";
        let w = wrap(g, &LexOptions::mariadb()).unwrap().unwrap();
        assert!(
            w.text
                .contains("SET `__sprig_rls_r` = (SELECT COUNT(*) FROM t); SET @sprig_rls")
        );
        assert_eq!(unwrap(&w.text), g);
        assert!(is_wrapped(&w.text) && !is_wrapped(g));
        // `RETURNS` with a type named like a characteristic start.
        let h = "CREATE DEFINER=`t42_u9`@`%` FUNCTION `h`() RETURNS int(11)\nNO SQL\nRETURN 1";
        let w = wrap(h, &LexOptions::mariadb()).unwrap().unwrap();
        assert!(
            w.text.contains("DECLARE `__sprig_rls_r` int(11);"),
            "{}",
            w.text
        );
    }
}
