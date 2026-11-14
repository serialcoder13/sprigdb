//! Splitting a `COM_QUERY` text into statements, and the file name of a
//! `LOAD DATA LOCAL INFILE`.
//!
//! The split is lexical: semicolons outside
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
            // A `$$ ... $$` body (logic functions and triggers, sink
            // scripts): its semicolons don't end the statement.
            b'$' if b.get(i + 1) == Some(&b'$') && dollar_starts(b, i) => i = skip_dollar(b, i),
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

/// Whether the `$$` at `i` opens a body (not part of a name like `a$$`).
fn dollar_starts(b: &[u8], i: usize) -> bool {
    i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'$')
}

/// Past the `$$ ... $$` body opening at `i` (the end of the text when it
/// isn't closed).
fn skip_dollar(b: &[u8], i: usize) -> usize {
    let mut j = i + 2;
    while j + 1 < b.len() {
        if b[j] == b'$' && b[j + 1] == b'$' {
            return j + 2;
        }
        j += 1;
    }
    b.len()
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
    matches!(b.get(i + 2), Some(b'!'))
        || (b.get(i + 2) == Some(&b'M') && b.get(i + 3) == Some(&b'!'))
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
fn skip_quoted(b: &[u8], i: usize, quote: u8) -> usize {
    let mut pos = i + 1;
    while pos < b.len() {
        let ch = b[pos];
        if ch == b'\\' && quote != b'`' {
            pos += 2;
        } else if ch == quote {
            if b.get(pos + 1) == Some(&quote) {
                pos += 2;
            } else {
                return pos + 1;
            }
        } else {
            pos += 1;
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
        Some("BEGIN") => {
            w.get(1).is_some_and(|x| x == "NOT") && w.get(2).is_some_and(|x| x == "ATOMIC")
        }
        _ => false,
    }
}

/// A `CONNECTION_ID()` call replaced by [`rewrite_connection_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Renamed {
    /// The column name the replacement produces.
    pub from: Bytes,
    /// The name the client's text gives the column (`CONNECTION_ID()` as
    /// it was written).
    pub to: Bytes,
}

/// Replaces every `CONNECTION_ID()` call outside quotes and comments with
/// the gateway's connection id `id`, the id the client's greeting, `SHOW
/// PROCESSLIST` and `KILL` use (the backend's would change with every
/// pooled connection). `None` when the text has no such call. The
/// replacement is a `BIGINT UNSIGNED` like the function's result; the
/// relay renames its column back to the text the client wrote.
#[must_use]
pub fn rewrite_connection_id(sql: &[u8], id: u32) -> Option<(Bytes, Vec<Renamed>)> {
    const NAME: &[u8] = b"CONNECTION_ID";
    let ident = |ch: u8| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$';
    let text = sql;
    let replacement = format!("CAST({id} AS UNSIGNED)");
    let mut out: Vec<u8> = Vec::new();
    let mut renamed = Vec::new();
    let mut copied = 0;
    let mut pos = 0;
    while pos < text.len() {
        match text[pos] {
            quote @ (b'\'' | b'"' | b'`') => pos = skip_quoted(text, pos, quote),
            b'#' => pos = skip_line(text, pos),
            b'-' if is_dash_comment(text, pos) => pos = skip_line(text, pos),
            b'/' if text.get(pos + 1) == Some(&b'*') && !is_executable_comment(text, pos) => {
                pos = skip_block(text, pos);
            }
            ch if ident(ch) => {
                let end = text[pos..]
                    .iter()
                    .position(|&ch| !ident(ch))
                    .map_or(text.len(), |off| pos + off);
                let before_ok = pos == 0 || !matches!(text[pos - 1], b'.' | b'@');
                if before_ok && text[pos..end].eq_ignore_ascii_case(NAME) {
                    // `(`, optional spaces, `)`.
                    let mut open = end;
                    while open < text.len() && text[open].is_ascii_whitespace() {
                        open += 1;
                    }
                    if text.get(open) == Some(&b'(') {
                        let mut close = open + 1;
                        while close < text.len() && text[close].is_ascii_whitespace() {
                            close += 1;
                        }
                        if text.get(close) == Some(&b')') {
                            out.extend_from_slice(&text[copied..pos]);
                            out.extend_from_slice(replacement.as_bytes());
                            renamed.push(Renamed {
                                from: Bytes::from(replacement.clone()),
                                to: Bytes::copy_from_slice(&text[pos..=close]),
                            });
                            copied = close + 1;
                            pos = close + 1;
                            continue;
                        }
                    }
                }
                pos = end;
            }
            _ => pos += 1,
        }
    }
    if renamed.is_empty() {
        return None;
    }
    out.extend_from_slice(&text[copied..]);
    Some((Bytes::from(out), renamed))
}

/// Replaces reads of gateway-owned variables the client set itself
/// (`@@wait_timeout`, `@@session.net_read_timeout`, `@@session_track_*`,
/// ...) in a `SELECT` with the client's values from `vars`: backend
/// connections keep the gateway's own values of these, so the
/// backend would answer with the gateway's. The relay renames each column
/// back to the text the client wrote. `None` when nothing changes. Global
/// reads (`@@global.x`) are the server's and stay.
#[must_use]
pub fn rewrite_gateway_vars(
    sql: &[u8],
    vars: &std::collections::BTreeMap<String, String>,
) -> Option<(Bytes, Vec<Renamed>)> {
    if vars.is_empty() || !first_word_is(sql, "SELECT") {
        return None;
    }
    let ident = |ch: u8| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$' || ch == b'.';
    let text = sql;
    let mut out: Vec<u8> = Vec::new();
    let mut renamed = Vec::new();
    let mut copied = 0;
    let mut pos = 0;
    while pos < text.len() {
        match text[pos] {
            quote @ (b'\'' | b'"' | b'`') => pos = skip_quoted(text, pos, quote),
            b'#' => pos = skip_line(text, pos),
            b'-' if is_dash_comment(text, pos) => pos = skip_line(text, pos),
            b'/' if text.get(pos + 1) == Some(&b'*') && !is_executable_comment(text, pos) => {
                pos = skip_block(text, pos);
            }
            b'@' if text.get(pos + 1) == Some(&b'@') => {
                let start = pos + 2;
                let end = text[start..]
                    .iter()
                    .position(|&ch| !ident(ch))
                    .map_or(text.len(), |off| start + off);
                let full = String::from_utf8_lossy(&text[start..end]).to_ascii_lowercase();
                let name = full
                    .strip_prefix("session.")
                    .or_else(|| full.strip_prefix("local."))
                    .unwrap_or(&full);
                if let Some(v) = vars.get(name).filter(|_| !name.contains('.')) {
                    let lit = if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) {
                        v.clone()
                    } else {
                        format!("'{}'", v.replace('\\', "\\\\").replace('\'', "''"))
                    };
                    out.extend_from_slice(&text[copied..pos]);
                    out.extend_from_slice(lit.as_bytes());
                    renamed.push(Renamed {
                        from: Bytes::from(lit),
                        to: Bytes::copy_from_slice(&text[pos..end]),
                    });
                    copied = end;
                }
                pos = end;
            }
            _ => pos += 1,
        }
    }
    if renamed.is_empty() {
        return None;
    }
    out.extend_from_slice(&text[copied..]);
    Some((Bytes::from(out), renamed))
}

/// Whether the statement's first word is `word`.
fn first_word_is(sql: &[u8], word: &str) -> bool {
    let Some(at) = first_word_at(sql, 0) else {
        return false;
    };
    let end = sql[at..]
        .iter()
        .position(|c| !c.is_ascii_alphabetic())
        .map_or(sql.len(), |o| at + o);
    sql[at..end].eq_ignore_ascii_case(word.as_bytes())
}

/// Whether `keyword` (upper case) appears as a word of `sql` outside
/// quotes and comments.
#[must_use]
pub fn has_keyword(sql: &[u8], keyword: &str) -> bool {
    let ident = |ch: u8| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$';
    let text = sql;
    let mut pos = 0;
    while pos < text.len() {
        match text[pos] {
            quote @ (b'\'' | b'"' | b'`') => pos = skip_quoted(text, pos, quote),
            b'#' => pos = skip_line(text, pos),
            b'-' if is_dash_comment(text, pos) => pos = skip_line(text, pos),
            b'/' if text.get(pos + 1) == Some(&b'*') && !is_executable_comment(text, pos) => {
                pos = skip_block(text, pos);
            }
            ch if ident(ch) => {
                let end = text[pos..]
                    .iter()
                    .position(|&ch| !ident(ch))
                    .map_or(text.len(), |off| pos + off);
                if text[pos..end].eq_ignore_ascii_case(keyword.as_bytes()) {
                    return true;
                }
                pos = end;
            }
            _ => pos += 1,
        }
    }
    false
}

/// Whether `stmt` is `SHOW [SESSION | LOCAL] STATUS ...` (session status,
/// whose connection rows the relay rewrites).
#[must_use]
pub fn is_session_status(stmt: &[u8]) -> bool {
    let Some(start) = first_word_at(stmt, 0) else {
        return false;
    };
    let w = words(stmt, start, 3);
    let w: Vec<&str> = w.iter().map(String::as_str).collect();
    matches!(
        w.as_slice(),
        ["SHOW", "STATUS", ..] | ["SHOW", "SESSION" | "LOCAL", "STATUS"]
    )
}

/// Whether `stmt` is `SHOW [GLOBAL | SESSION | LOCAL] VARIABLES ...`, whose
/// version rows the relay rewrites with the persona's.
#[must_use]
pub fn is_show_variables(stmt: &[u8]) -> bool {
    let Some(start) = first_word_at(stmt, 0) else {
        return false;
    };
    let w = words(stmt, start, 3);
    let w: Vec<&str> = w.iter().map(String::as_str).collect();
    matches!(
        w.as_slice(),
        ["SHOW", "VARIABLES", ..] | ["SHOW", "GLOBAL" | "SESSION" | "LOCAL", "VARIABLES"]
    )
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
fn unescape(b: &[u8], i: usize, quote: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pos = i + 1;
    while pos < b.len() {
        let ch = b[pos];
        if ch == b'\\' {
            let Some(&esc) = b.get(pos + 1) else { break };
            out.push(match esc {
                b'0' => 0,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'b' => 8,
                b'Z' => 26,
                other => other,
            });
            pos += 2;
        } else if ch == quote {
            if b.get(pos + 1) == Some(&quote) {
                out.push(quote);
                pos += 2;
            } else {
                break;
            }
        } else {
            out.push(ch);
            pos += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_vars_read_the_clients_values() {
        let mut vars = std::collections::BTreeMap::new();
        vars.insert("wait_timeout".to_owned(), "600".to_owned());
        vars.insert("session_track_schema".to_owned(), "OFF".to_owned());
        let (sql, renames) = rewrite_gateway_vars(
            b"SELECT @@wait_timeout, @@SESSION.session_track_schema, @@global.wait_timeout, '@@wait_timeout'",
            &vars,
        )
        .unwrap();
        assert_eq!(
            &sql[..],
            b"SELECT 600, 'OFF', @@global.wait_timeout, '@@wait_timeout'"
        );
        assert_eq!(&renames[0].to[..], b"@@wait_timeout");
        assert_eq!(&renames[1].from[..], b"'OFF'");
        assert!(rewrite_gateway_vars(b"SET @@wait_timeout = 5", &vars).is_none());
        assert!(rewrite_gateway_vars(b"SELECT @@net_read_timeout", &vars).is_none());
    }

    use super::*;

    fn pieces(s: &str) -> Vec<String> {
        split(&Bytes::copy_from_slice(s.as_bytes()))
            .iter()
            .map(|b| String::from_utf8_lossy(b).trim().to_owned())
            .collect()
    }

    #[test]
    fn dollar_bodies_stay_whole() {
        let sql = Bytes::from_static(
            b"CREATE SINK s FROM STREAM ev TO SCRIPT LANGUAGE javascript AS $$ \
              export function deliver(b, c) { const x = 1; return true; } $$; SELECT 1",
        );
        let v = split(&sql);
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(v[0].ends_with(b"$$"));
        assert_eq!(&v[1][..], b" SELECT 1");
        // `$$` inside a name isn't a body.
        assert_eq!(
            split(&Bytes::from_static(b"SELECT a$$b; SELECT 2")).len(),
            2
        );
    }

    #[test]
    fn splits_on_semicolons_outside_quotes_and_comments() {
        assert_eq!(pieces("SELECT 1"), ["SELECT 1"]);
        assert_eq!(pieces("SELECT 1;"), ["SELECT 1"]);
        assert_eq!(
            pieces("SELECT 1; SELECT 2, 'two'"),
            ["SELECT 1", "SELECT 2, 'two'"]
        );
        assert_eq!(
            pieces("SELECT ';', \"a;b\", `c;d`; DO 1"),
            ["SELECT ';', \"a;b\", `c;d`", "DO 1"]
        );
        assert_eq!(
            pieces("SELECT 'it''s;' ; DO 1"),
            ["SELECT 'it''s;'", "DO 1"]
        );
        assert_eq!(pieces("SELECT 'a\\';b'; DO 1"), ["SELECT 'a\\';b'", "DO 1"]);
        assert_eq!(
            pieces("SELECT 1 -- x; y\n; DO 2"),
            ["SELECT 1 -- x; y", "DO 2"]
        );
        assert_eq!(
            pieces("SELECT 1 /* ; */; # c;\nDO 2"),
            ["SELECT 1 /* ; */", "# c;\nDO 2"]
        );
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
        assert_eq!(
            p,
            [
                "DO 1",
                "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END; CALL p()"
            ]
        );
        let p = pieces("CREATE DEFINER=`root`@`%` FUNCTION f() RETURNS INT BEGIN RETURN 1; END");
        assert_eq!(p.len(), 1);
        let p = pieces(
            "CREATE OR REPLACE TRIGGER t BEFORE INSERT ON x FOR EACH ROW BEGIN SET @a=1; END",
        );
        assert_eq!(p.len(), 1);
        let p = pieces("BEGIN NOT ATOMIC SELECT 1; SELECT 2; END");
        assert_eq!(p.len(), 1);
        // Not compound: a table whose column is named like a keyword.
        assert_eq!(pieces("CREATE TABLE t (procedure_id INT); DO 1").len(), 2);
        assert_eq!(pieces("BEGIN; SELECT 1").len(), 2);
    }

    #[test]
    fn connection_id_becomes_the_gateways() {
        let (sql, r) = rewrite_connection_id(
            b"SELECT CONNECTION_ID(), connection_id ( ) AS c, 'CONNECTION_ID()'",
            42,
        )
        .unwrap();
        assert_eq!(
            &sql[..],
            &b"SELECT CAST(42 AS UNSIGNED), CAST(42 AS UNSIGNED) AS c, 'CONNECTION_ID()'"[..]
        );
        assert_eq!(r.len(), 2);
        assert_eq!(&r[0].to[..], b"CONNECTION_ID()");
        assert_eq!(&r[1].to[..], b"connection_id ( )");
        assert_eq!(&r[0].from[..], b"CAST(42 AS UNSIGNED)");
        assert!(rewrite_connection_id(b"SELECT 1 -- CONNECTION_ID()", 1).is_none());
        assert!(
            rewrite_connection_id(b"SELECT my_connection_id(), t.connection_id FROM t", 1)
                .is_none()
        );
        assert!(rewrite_connection_id(b"KILL 5", 1).is_none());
    }

    #[test]
    fn keywords() {
        assert!(has_keyword(
            b"SELECT v FROM t WHERE id = 1 FOR UPDATE nowait",
            "NOWAIT"
        ));
        assert!(!has_keyword(
            b"SELECT 'NOWAIT', nowaiting FROM t -- NOWAIT",
            "NOWAIT"
        ));
    }

    #[test]
    fn session_status() {
        assert!(is_session_status(b"SHOW SESSION STATUS LIKE 'Ssl_cipher'"));
        assert!(is_session_status(
            b"show status where Variable_name = 'Compression'"
        ));
        assert!(is_session_status(b"SHOW LOCAL STATUS"));
        assert!(!is_session_status(b"SHOW GLOBAL STATUS LIKE 'Ssl_cipher'"));
        assert!(!is_session_status(b"SHOW VARIABLES LIKE 'x'"));
        assert!(is_show_variables(b"SHOW VARIABLES LIKE 'version%'"));
        assert!(is_show_variables(
            b"show global variables where Variable_name = 'version'"
        ));
        assert!(!is_show_variables(b"SHOW STATUS"));
    }

    #[test]
    fn local_infile_names() {
        let n = |s: &str| local_infile_name(s.as_bytes()).map(|v| String::from_utf8(v).unwrap());
        assert_eq!(
            n("LOAD DATA LOCAL INFILE 'f.csv' INTO TABLE t"),
            Some("f.csv".into())
        );
        assert_eq!(
            n("load data low_priority local infile \"/tmp/a b.csv\" into table t"),
            Some("/tmp/a b.csv".into())
        );
        assert_eq!(
            n("LOAD DATA LOCAL INFILE 'it''s\\\\x' INTO TABLE t"),
            Some("it's\\x".into())
        );
        assert_eq!(
            n("  /* c */ LOAD XML LOCAL INFILE 'x.xml' INTO TABLE t"),
            Some("x.xml".into())
        );
        assert_eq!(n("LOAD DATA INFILE 'f.csv' INTO TABLE t"), None);
        assert_eq!(n("SELECT 'LOAD DATA LOCAL INFILE'"), None);
    }
}
