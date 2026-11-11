//! DDL rules of the MySQL persona that change the statement text (chapter
//! 28, "DDL and types"; build step 10; R4 round 3).
//!
//! - **`NOT NULL` on a generated column** (gaps row 49). MySQL accepts
//!   `c INT AS (expr) [VIRTUAL | STORED] NOT NULL` (and `NULL`); MariaDB
//!   allows neither attribute there and fails with 1064. The attribute
//!   goes, and `NOT NULL` becomes a table check named
//!   [`NN_PREFIX`]`<column>`: `CHECK (`c` IS NOT NULL)`. A write that makes
//!   the value `NULL` fails as on MySQL (MariaDB's 4025 on that check reads
//!   as MySQL's 1048, [`super::errmap`]); `SHOW CREATE TABLE` puts
//!   `NOT NULL` back on the column and leaves the check out
//!   ([`super::schema::mysql_create_table`]); `information_schema.COLUMNS`
//!   reads `IS_NULLABLE = 'NO'`, and the check is left out of
//!   `CHECK_CONSTRAINTS` and `TABLE_CONSTRAINTS`. `CREATE TABLE` and
//!   `ALTER TABLE ... ADD [COLUMN]`; `MODIFY` and `CHANGE` keep MariaDB's
//!   1064. A column name longer than 54 characters keeps it too (the
//!   check's name must fit 64).
//! - **A database's default collation** (gaps row 58). `CREATE` or
//!   `ALTER DATABASE ... CHARACTER SET cs` without `COLLATE` gets MySQL
//!   8.4's default collation of `cs` where MariaDB's differs (`utf8mb3`,
//!   `ucs2`, `utf16`, `utf32`: `*_general_ci`). Database DDL runs on the
//!   provisioning account's connection, which doesn't carry the session's
//!   `character_set_collations` (the backend session gives every other
//!   statement MySQL's defaults, chapter 06).

use crate::lex::{self, LexOptions, Token, TokenKind};

/// The name prefix of the checks that hold a generated column's
/// `NOT NULL`. Chapter 07 reserves `_sprig_`.
pub const NN_PREFIX: &str = "_sprig_nn_";

/// The longest column name whose `NOT NULL` check fits MariaDB's 64
/// characters.
const NN_MAX_COLUMN: usize = 64 - NN_PREFIX.len();

/// The check that holds `column`'s `NOT NULL`.
#[must_use]
pub fn nn_check_name(column: &str) -> String {
    format!("{NN_PREFIX}{column}")
}

/// The column a [`NN_PREFIX`] check name stands for.
#[must_use]
pub fn nn_column(check: &str) -> Option<&str> {
    check.strip_prefix(NN_PREFIX).filter(|c| !c.is_empty())
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

struct Sig<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl Sig<'_> {
    fn word(&self, j: usize, w: &[u8]) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w))
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && self.sql[tk.start as usize] == c)
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    /// The `)` matching the `(` at `from`.
    fn matching(&self, from: usize) -> Option<usize> {
        let mut d = 0;
        for k in from..self.t.len() {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    /// `[a, b)` split at depth-0 commas (relative to `a`).
    fn split(&self, a: usize, b: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut d = 0;
        let mut start = a;
        for k in a..b {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
            } else if d == 0 && self.punct(k, b',') {
                out.push((start, k));
                start = k + 1;
            }
        }
        out.push((start, b));
        out
    }
}

/// A column definition `[a, b)` (`a` is the column's name): the token
/// ranges of `NULL` / `NOT NULL` after a generation clause, and whether
/// one of them is `NOT NULL`. `None` when the column isn't generated or
/// has neither.
fn generated_nullability(s: &Sig<'_>, a: usize, b: usize) -> Option<(Vec<(usize, usize)>, bool)> {
    // `AS (` at depth 0 of the definition, after the type.
    let mut k = a + 1;
    let mut d = 0;
    let mut gen_end = None;
    while k < b {
        if s.punct(k, b'(') {
            d += 1;
        } else if s.punct(k, b')') {
            d -= 1;
        } else if d == 0 && s.word(k, b"AS") && s.punct(k + 1, b'(') {
            gen_end = Some(s.matching(k + 1)?);
            break;
        }
        k += 1;
    }
    let mut k = gen_end? + 1;
    let mut drops = Vec::new();
    let mut not_null = false;
    while k < b {
        if s.punct(k, b'(') {
            // `COMMENT`, `CHECK (...)`, `REFERENCES t (...)`: skip the group.
            k = s.matching(k)? + 1;
            continue;
        }
        if s.word(k, b"NOT") && s.word(k + 1, b"NULL") {
            drops.push((k, k + 1));
            not_null = true;
            k += 2;
            continue;
        }
        if s.word(k, b"NULL") {
            drops.push((k, k));
            k += 1;
            continue;
        }
        k += 1;
    }
    (!drops.is_empty()).then_some((drops, not_null))
}

/// Edits as `(from byte, to byte, text)`, applied in order.
fn apply(sql: &[u8], mut edits: Vec<(usize, usize, String)>) -> Vec<u8> {
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 64);
    let mut at = 0;
    for (from, to, text) in edits {
        out.extend_from_slice(&sql[at..from]);
        out.extend_from_slice(text.as_bytes());
        at = to;
    }
    out.extend_from_slice(&sql[at..]);
    out
}

/// `NOT NULL` and `NULL` on generated columns (see the module docs).
/// `None` when nothing changes.
#[must_use]
pub fn generated_not_null(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let has = |n: &[u8]| sql.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n));
    if !has(b"NULL") || !has(b"AS") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let s = Sig {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let drop_tokens = |edits: &mut Vec<(usize, usize, String)>, drops: &[(usize, usize)]| {
        for &(x, y) in drops {
            // The attribute and the space before it.
            let from = if x > 0 { s.t[x - 1].end as usize } else { s.t[x].start as usize };
            edits.push((from, s.t[y].end as usize, String::new()));
        }
    };
    if s.word(0, b"CREATE") {
        let mut j = 1;
        if s.word(j, b"OR") && s.word(j + 1, b"REPLACE") {
            j += 2;
        }
        if s.word(j, b"TEMPORARY") {
            j += 1;
        }
        if !s.word(j, b"TABLE") {
            return None;
        }
        while j < s.t.len() && !s.punct(j, b'(') {
            j += 1;
        }
        let open = j;
        let close = s.matching(open)?;
        let mut checks = String::new();
        for (a, b) in s.split(open + 1, close) {
            if a >= b || !s.t[a].kind.is_ident_like() || element_keyword(&s, a) {
                continue;
            }
            let Some(col) = s.ident(a) else { continue };
            let Some((drops, not_null)) = generated_nullability(&s, a, b) else {
                continue;
            };
            if not_null && col.chars().count() > NN_MAX_COLUMN {
                continue;
            }
            drop_tokens(&mut edits, &drops);
            if not_null {
                checks.push_str(&format!(
                    ", CONSTRAINT {} CHECK ({} IS NOT NULL)",
                    quote(&nn_check_name(&col)),
                    quote(&col)
                ));
            }
        }
        if !checks.is_empty() {
            let at = s.t[close].start as usize;
            edits.push((at, at, checks));
        }
    } else if s.word(0, b"ALTER") {
        let mut j = 1;
        if s.word(j, b"ONLINE") || s.word(j, b"IGNORE") {
            j += 1;
        }
        if !s.word(j, b"TABLE") {
            return None;
        }
        // The specification list starts after the table name.
        j += 1;
        if s.word(j, b"IF") && s.word(j + 1, b"EXISTS") {
            j += 2;
        }
        j += if s.punct(j + 1, b'.') { 3 } else { 1 };
        let mut adds = String::new();
        for (a, b) in s.split(j, s.t.len()) {
            if !s.word(a, b"ADD") {
                continue;
            }
            let mut c = a + 1;
            if s.word(c, b"COLUMN") {
                c += 1;
            }
            if s.word(c, b"IF") && s.word(c + 1, b"NOT") && s.word(c + 2, b"EXISTS") {
                c += 3;
            }
            if c >= b || !s.t[c].kind.is_ident_like() || element_keyword(&s, c) {
                continue;
            }
            let Some(col) = s.ident(c) else { continue };
            let Some((drops, not_null)) = generated_nullability(&s, c, b) else {
                continue;
            };
            if not_null && col.chars().count() > NN_MAX_COLUMN {
                continue;
            }
            drop_tokens(&mut edits, &drops);
            if not_null {
                adds.push_str(&format!(
                    ", ADD CONSTRAINT {} CHECK ({} IS NOT NULL)",
                    quote(&nn_check_name(&col)),
                    quote(&col)
                ));
            }
        }
        if !adds.is_empty() {
            let last = s.t.iter().rposition(|t| !(t.kind == TokenKind::Punct && sql[t.start as usize] == b';'))?;
            let at = s.t[last].end as usize;
            edits.push((at, at, adds));
        }
    } else {
        return None;
    }
    (!edits.is_empty()).then(|| apply(sql, edits))
}

/// Whether the element at `a` starts with a keyword (a key, constraint
/// or period), not a column's name.
fn element_keyword(s: &Sig<'_>, a: usize) -> bool {
    s.t[a].kind == TokenKind::Word
        && [
            &b"PRIMARY"[..],
            b"UNIQUE",
            b"KEY",
            b"INDEX",
            b"FOREIGN",
            b"FULLTEXT",
            b"SPATIAL",
            b"CONSTRAINT",
            b"CHECK",
            b"PERIOD",
        ]
        .iter()
        .any(|w| s.word(a, w))
}

/// MySQL 8.4's default collation of a character set where MariaDB 11.8's
/// differs.
#[must_use]
pub fn mysql_default_collation(charset: &str) -> Option<&'static str> {
    Some(match charset.to_ascii_lowercase().as_str() {
        "utf8mb3" | "utf8" => "utf8mb3_general_ci",
        "ucs2" => "ucs2_general_ci",
        "utf16" => "utf16_general_ci",
        "utf32" => "utf32_general_ci",
        _ => return None,
    })
}

/// `CREATE` / `ALTER DATABASE | SCHEMA ... CHARACTER SET cs` without
/// `COLLATE`: MySQL's default collation of `cs` added (see the module
/// docs). `None` when nothing changes.
#[must_use]
pub fn database_collation(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let has = |n: &[u8]| sql.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n));
    if !(has(b"DATABASE") || has(b"SCHEMA")) || !(has(b"CHARSET") || has(b"CHARACTER")) || has(b"COLLATE") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let s = Sig {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    if !(s.word(0, b"CREATE") || s.word(0, b"ALTER")) || !(s.word(1, b"DATABASE") || s.word(1, b"SCHEMA")) {
        return None;
    }
    for k in 2..s.t.len() {
        let after = if s.word(k, b"CHARSET") {
            k + 1
        } else if s.word(k, b"CHARACTER") && s.word(k + 1, b"SET") {
            k + 2
        } else {
            continue;
        };
        let v = if s.punct(after, b'=') || (s.t.get(after).is_some_and(|t| t.kind == TokenKind::Op && t.bytes(sql) == b"=")) {
            after + 1
        } else {
            after
        };
        let tk = s.t.get(v)?;
        let cs = match tk.kind {
            TokenKind::String => String::from_utf8_lossy(&lex::string_value(tk.bytes(sql), opts.no_backslash_escapes)?).into_owned(),
            _ if tk.kind.is_ident_like() => s.ident(v)?,
            _ => return None,
        };
        let coll = mysql_default_collation(&cs)?;
        let at = tk.end as usize;
        return Some(apply(sql, vec![(at, at, format!(" COLLATE {coll}"))]));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o() -> LexOptions {
        LexOptions::mysql84()
    }

    fn nn(s: &str) -> Option<String> {
        generated_not_null(s.as_bytes(), &o()).map(|v| String::from_utf8(v).unwrap())
    }

    fn db(s: &str) -> Option<String> {
        database_collation(s.as_bytes(), &o()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn generated_not_null_becomes_a_check() {
        assert_eq!(
            nn("CREATE TABLE g (a INT, b INT AS (a+1) NOT NULL, c INT GENERATED ALWAYS AS (a*2) STORED NOT NULL, d INT AS (a) VIRTUAL NULL)")
                .unwrap(),
            "CREATE TABLE g (a INT, b INT AS (a+1), c INT GENERATED ALWAYS AS (a*2) STORED, d INT AS (a) VIRTUAL, \
             CONSTRAINT `_sprig_nn_b` CHECK (`b` IS NOT NULL), CONSTRAINT `_sprig_nn_c` CHECK (`c` IS NOT NULL))"
        );
        // Other attributes stay; a NULL inside the expression is the
        // expression's.
        assert_eq!(
            nn("CREATE TABLE t (id INT PRIMARY KEY, `x y` VARCHAR(5) AS (IFNULL(n, NULL)) STORED NOT NULL COMMENT 'c' UNIQUE)")
                .unwrap(),
            "CREATE TABLE t (id INT PRIMARY KEY, `x y` VARCHAR(5) AS (IFNULL(n, NULL)) STORED COMMENT 'c' UNIQUE, \
             CONSTRAINT `_sprig_nn_x y` CHECK (`x y` IS NOT NULL))"
        );
        // Plain columns and keys are left alone.
        assert_eq!(nn("CREATE TABLE t (a INT NOT NULL, b INT AS (a) VIRTUAL)"), None);
        assert_eq!(nn("CREATE TABLE t (a INT NOT NULL DEFAULT 1, KEY k (a))"), None);
        assert_eq!(nn("CREATE TABLE t AS SELECT 1 AS a"), None);
        assert_eq!(nn("SELECT a AS b FROM t WHERE c IS NOT NULL"), None);
    }

    #[test]
    fn alter_add_generated_not_null() {
        assert_eq!(
            nn("ALTER TABLE s.t ADD COLUMN e INT AS (a+5) VIRTUAL NOT NULL AFTER a, ADD INDEX (a)").unwrap(),
            "ALTER TABLE s.t ADD COLUMN e INT AS (a+5) VIRTUAL AFTER a, ADD INDEX (a), \
             ADD CONSTRAINT `_sprig_nn_e` CHECK (`e` IS NOT NULL)"
        );
        assert_eq!(
            nn("ALTER TABLE t ADD f INT AS (a) NULL;").unwrap(),
            "ALTER TABLE t ADD f INT AS (a);"
        );
        assert_eq!(nn("ALTER TABLE t MODIFY a INT NOT NULL"), None);
        assert_eq!(nn("ALTER TABLE t ADD CONSTRAINT c CHECK (a IS NOT NULL)"), None);
    }

    #[test]
    fn names_round_trip() {
        assert_eq!(nn_column(&nn_check_name("b")), Some("b"));
        assert_eq!(nn_column("t_chk_1"), None);
    }

    #[test]
    fn database_charsets_get_mysqls_default_collation() {
        assert_eq!(
            db("CREATE DATABASE d CHARACTER SET utf8mb3").unwrap(),
            "CREATE DATABASE d CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci"
        );
        assert_eq!(
            db("create schema if not exists d default charset = 'utf8'").unwrap(),
            "create schema if not exists d default charset = 'utf8' COLLATE utf8mb3_general_ci"
        );
        assert_eq!(
            db("ALTER DATABASE d CHARACTER SET ucs2").unwrap(),
            "ALTER DATABASE d CHARACTER SET ucs2 COLLATE ucs2_general_ci"
        );
        assert_eq!(db("CREATE DATABASE d CHARACTER SET utf8mb4"), None);
        assert_eq!(db("CREATE DATABASE d CHARACTER SET utf8mb3 COLLATE utf8mb3_bin"), None);
        assert_eq!(db("CREATE TABLE t (a VARCHAR(5) CHARACTER SET utf8mb3)"), None);
    }
}
