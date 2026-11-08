//! Splitting a `COM_QUERY` text into statements (chapter 05,
//! "Multi-statements and multiple results"), and the file name of a
//! `LOAD DATA LOCAL INFILE` (chapter 05, "LOAD DATA LOCAL INFILE").
//!
//! Until the parser (chapter 17) the split is lexical: semicolons outside
//! quotes, back quotes and comments end a statement. A compound statement
//! (`CREATE PROCEDURE`, `FUNCTION`, `TRIGGER`, `EVENT`, `PACKAGE`, or
//! `BEGIN NOT ATOMIC`) has semicolons inside its body that a lexer can't
//! tell apart, so from the first one on the rest of the text stays one
//! piece and the backend splits it (its results still stream one by one).

use bytes::Bytes;

/// Words that, after `CREATE`, start a stored program with a body.
const PROGRAMS: &[&str] = &["PROCEDURE", "FUNCTION", "TRIGGER", "EVENT", "PACKAGE"];

/// How far `CREATE ...` is scanned for one of [`PROGRAMS`] (`CREATE OR
/// REPLACE DEFINER = a@b AGGREGATE FUNCTION` is the longest prefix).
const PROGRAM_SCAN_WORDS: usize = 12;

/// The statements of `sql`, as slices of it. Pieces holding only
/// whitespace and plain comments are dropped (a trailing `;` makes no
/// empty statement); executable comments (`/*! ... */`) count as text.
/// An empty result means the text has no statement at all.
#[must_use]
pub fn split(sql: &Bytes) -> Vec<Bytes> {
    let b = &sql[..];
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    let mut at_stmt_start = true;
    while i < b.len() {
        if at_stmt_start {
            if let Some(word_at) = first_word_at(b, i) {
                at_stmt_start = false;
                if starts_compound(b, word_at) {
                    break;
                }
            } else {
                // Only whitespace and comments up to the end.
                break;
            }
        }
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => i = skip_quoted(b, i, q),
            b'#' => i = skip_line(b, i),
            b'-' if is_dash_comment(b, i) => i = skip_line(b, i),
            b'/' if b.get(i + 1) == Some(&b'*') => i = skip_block(b, i),
            b';' => {
                push_piece(&mut out, sql, start, i);
                i += 1;
                start = i;
                at_stmt_start = true;
            }
            _ => i += 1,
        }
    }
    push_piece(&mut out, sql, start, b.len());
    out
}

fn push_piece(out: &mut Vec<Bytes>, sql: &Bytes, start: usize, end: usize) {
    if start < end && has_text(&sql[start..end]) {
        out.push(sql.slice(start..end));
    }
}

/// Whether `b` holds anything but whitespace and plain comments.
fn has_text(b: &[u8]) -> bool {
    first_word_at(b, 0).is_some()
}

/// The offset of the first byte of text at or after `i`, skipping
/// whitespace and plain comments; `None` if there is none.
fn first_word_at(b: &[u8], mut i: usize) -> Option<usize> {
    while i < b.len() {
        match b[i] {
            c if c.is_ascii_whitespace() => i += 1,
            b'#' => i = skip_line(b, i),
            b'-' if is_dash_comment(b, i) => i = skip_line(b, i),
            b'/' if b.get(i + 1) == Some(&b'*') => {
                if is_executable_comment(b, i) {
                    return Some(i);
                }
                i = skip_block(b, i);
            }
            _ => return Some(i),
        }
    }
    None
}

/// `-- ` (a double dash then whitespace or the end) starts a comment.
fn is_dash_comment(b: &[u8], i: usize) -> bool {
    b.get(i + 1) == Some(&b'-') && b.get(i + 2).is_none_or(u8::is_ascii_whitespace)
}

/// `/*!` and `/*M!` comments are statement text to the server.
fn is_executable_comment(b: &[u8], i: usize) -> bool {
    matches!(b.get(i + 2), Some(b'!')) || (b.get(i + 2) == Some(&b'M') && b.get(i + 3) == Some(&b'!'))
}

fn skip_line(b: &[u8], i: usize) -> usize {
    b[i..]
        .iter()
        .position(|&c| c == b'\n')
        .map_or(b.len(), |p| i + p + 1)
}

fn skip_block(b: &[u8], i: usize) -> usize {
    b[i + 2..]
        .windows(2)
        .position(|w| w == b"*/")
        .map_or(b.len(), |p| i + 2 + p + 2)
}

/// Skips a quoted string or identifier starting at `i` (quote `q`):
/// doubled quotes, and backslash escapes in strings.
fn skip_quoted(b: &[u8], i: usize, q: u8) -> usize {
    let mut j = i + 1;
    while j < b.len() {
        let c = b[j];
        if c == b'\\' && q != b'`' {
            j += 2;
        } else if c == q {
            if b.get(j + 1) == Some(&q) {
                j += 2;
            } else {
                return j + 1;
            }
        } else {
            j += 1;
        }
    }
    b.len()
}

/// The upper-cased words of `b` from `i`, up to the first `(`, `;` or
/// quote, at most `max` of them.
fn words(b: &[u8], mut i: usize, max: usize) -> Vec<String> {
    let mut out = Vec::new();
    while i < b.len() && out.len() < max {
        let Some(at) = first_word_at(b, i) else { break };
        i = at;
        let c = b[i];
        if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' {
            let end = b[i..]
                .iter()
                .position(|&c| !(c.is_ascii_alphanumeric() || c == b'_' || c == b'$'))
                .map_or(b.len(), |p| i + p);
            out.push(String::from_utf8_lossy(&b[i..end]).to_ascii_uppercase());
            i = end;
        } else if matches!(c, b'(' | b';') {
            break;
        } else if matches!(c, b'\'' | b'"' | b'`') {
            i = skip_quoted(b, i, c);
        } else if c == b'/' {
            // An executable comment at a statement start: not a program.
            break;
        } else {
            i += 1;
        }
    }
    out
}

/// Whether the statement starting at `i` is a compound statement.
fn starts_compound(b: &[u8], i: usize) -> bool {
    let w = words(b, i, PROGRAM_SCAN_WORDS);
    match w.first().map(String::as_str) {
        Some("CREATE") => w.iter().skip(1).any(|x| PROGRAMS.contains(&x.as_str())),
        Some("BEGIN") => w.get(1).is_some_and(|x| x == "NOT") && w.get(2).is_some_and(|x| x == "ATOMIC"),
        _ => false,
    }
}

/// For `LOAD DATA [LOW_PRIORITY | CONCURRENT] LOCAL INFILE 'name'` (and
/// `LOAD XML ... LOCAL INFILE`): the file name, unescaped as the server
/// will send it back in its request. `None` for any other statement.
#[must_use]
pub fn local_infile_name(stmt: &[u8]) -> Option<Vec<u8>> {
    let start = first_word_at(stmt, 0)?;
    let w = words(stmt, start, 6);
    if w.first().map(String::as_str) != Some("LOAD") {
        return None;
    }
    let local = w.iter().position(|x| x == "LOCAL")?;
    if w.get(local + 1).map(String::as_str) != Some("INFILE") {
        return None;
    }
    // The literal follows INFILE.
    let upper: Vec<u8> = stmt.to_ascii_uppercase();
    let at = find_word(&upper, b"INFILE")? + b"INFILE".len();
    let q = first_word_at(stmt, at)?;
    let quote = stmt[q];
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    Some(unescape(stmt, q, quote))
}

/// The offset of `word` as a whole word in `b` (outside nothing: callers
/// look for keywords that precede any literal).
fn find_word(b: &[u8], word: &[u8]) -> Option<usize> {
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    (0..b.len().saturating_sub(word.len() - 1)).find(|&i| {
        b[i..].starts_with(word)
            && (i == 0 || !ident(b[i - 1]))
            && b.get(i + word.len()).is_none_or(|&c| !ident(c))
    })
}

/// The value of the string literal quoted with `q` at `i`.
fn unescape(b: &[u8], i: usize, q: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let mut j = i + 1;
    while j < b.len() {
        let c = b[j];
        if c == b'\\' {
            let Some(&e) = b.get(j + 1) else { break };
            out.push(match e {
                b'0' => 0,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'b' => 8,
                b'Z' => 26,
                other => other,
            });
            j += 2;
        } else if c == q {
            if b.get(j + 1) == Some(&q) {
                out.push(q);
                j += 2;
            } else {
                break;
            }
        } else {
            out.push(c);
            j += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pieces(s: &str) -> Vec<String> {
        split(&Bytes::copy_from_slice(s.as_bytes()))
            .iter()
            .map(|b| String::from_utf8_lossy(b).trim().to_owned())
            .collect()
    }

    #[test]
    fn splits_on_semicolons_outside_quotes_and_comments() {
        assert_eq!(pieces("SELECT 1"), ["SELECT 1"]);
        assert_eq!(pieces("SELECT 1;"), ["SELECT 1"]);
        assert_eq!(pieces("SELECT 1; SELECT 2, 'two'"), ["SELECT 1", "SELECT 2, 'two'"]);
        assert_eq!(pieces("SELECT ';', \"a;b\", `c;d`; DO 1"), ["SELECT ';', \"a;b\", `c;d`", "DO 1"]);
        assert_eq!(pieces("SELECT 'it''s;' ; DO 1"), ["SELECT 'it''s;'", "DO 1"]);
        assert_eq!(pieces("SELECT 'a\\';b'; DO 1"), ["SELECT 'a\\';b'", "DO 1"]);
        assert_eq!(pieces("SELECT 1 -- x; y\n; DO 2"), ["SELECT 1 -- x; y", "DO 2"]);
        assert_eq!(pieces("SELECT 1 /* ; */; # c;\nDO 2"), ["SELECT 1 /* ; */", "# c;\nDO 2"]);
        assert_eq!(pieces("SELECT 1; -- done"), ["SELECT 1"]);
        assert_eq!(pieces("SELECT 1;;  ;"), ["SELECT 1"]);
        assert!(pieces("  -- nothing\n").is_empty());
        assert!(pieces("").is_empty());
        // 3-2 is not a comment.
        assert_eq!(pieces("SELECT 3--2; DO 1"), ["SELECT 3--2", "DO 1"]);
    }

    #[test]
    fn executable_comments_are_statements() {
        assert_eq!(
            pieces("/*!40101 SET NAMES utf8mb4 */; /*M!100100 SET @a=1 */;"),
            ["/*!40101 SET NAMES utf8mb4 */", "/*M!100100 SET @a=1 */"]
        );
    }

    #[test]
    fn compound_statements_stay_whole() {
        let p = pieces("DO 1; CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END; CALL p()");
        assert_eq!(p, ["DO 1", "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END; CALL p()"]);
        let p = pieces("CREATE DEFINER=`root`@`%` FUNCTION f() RETURNS INT BEGIN RETURN 1; END");
        assert_eq!(p.len(), 1);
        let p = pieces("CREATE OR REPLACE TRIGGER t BEFORE INSERT ON x FOR EACH ROW BEGIN SET @a=1; END");
        assert_eq!(p.len(), 1);
        let p = pieces("BEGIN NOT ATOMIC SELECT 1; SELECT 2; END");
        assert_eq!(p.len(), 1);
        // Not compound: a table whose column is named like a keyword.
        assert_eq!(pieces("CREATE TABLE t (procedure_id INT); DO 1").len(), 2);
        assert_eq!(pieces("BEGIN; SELECT 1").len(), 2);
    }

    #[test]
    fn local_infile_names() {
        let n = |s: &str| local_infile_name(s.as_bytes()).map(|v| String::from_utf8(v).unwrap());
        assert_eq!(n("LOAD DATA LOCAL INFILE 'f.csv' INTO TABLE t"), Some("f.csv".into()));
        assert_eq!(n("load data low_priority local infile \"/tmp/a b.csv\" into table t"), Some("/tmp/a b.csv".into()));
        assert_eq!(n("LOAD DATA LOCAL INFILE 'it''s\\\\x' INTO TABLE t"), Some("it's\\x".into()));
        assert_eq!(n("  /* c */ LOAD XML LOCAL INFILE 'x.xml' INTO TABLE t"), Some("x.xml".into()));
        assert_eq!(n("LOAD DATA INFILE 'f.csv' INTO TABLE t"), None);
        assert_eq!(n("SELECT 'LOAD DATA LOCAL INFILE'"), None);
    }
}
