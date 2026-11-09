//! The translator's R0 subset (chapter 28, build steps 1 to 5): what lets
//! MySQL and MariaDB clients connect and introspect through either persona.
//!
//! - Step 1, executable comments per persona: [`crate::lex::versioned`]
//!   (the lexer's rules and neutralization) and
//!   [`crate::lex::versioned::expand_for_mariadb`] (MySQL-only comments whose
//!   body MariaDB understands).
//! - Step 2, emulated variables and version strings: [`vars`].
//! - Step 3, the collation map with metadata ids: [`collation`].
//! - Step 4, the core `information_schema` providers and `SHOW` rewrites:
//!   [`schema`] (persona shapes on top of chapter 07's inline views), and
//!   [`meta`] for result metadata (MySQL's `JSON` type, 245).
//! - Step 5, error mapping: [`errmap`].
//!
//! R4 adds MySQL 8.4's `sql_mode` ([`sqlmode`]), `max_execution_time`
//! and statement hints ([`setvar`]), and MySQL's wording of errors
//! ([`errmap::reworded`], 1062 key names). The rule framework and
//! translation cache of step 6 (`sprig_sql::translate`) are not built:
//! the rules stay text rewrites on the lexer.

pub mod collation;
pub mod errmap;
pub mod geo;
pub mod keywords;
pub mod meta;
pub mod schema;
pub mod setvar;
pub mod sqlmode;
pub mod vars;

use crate::lex::{self, LexOptions, TokenKind};

/// `CAST(<operand> AS JSON)` with a parameter, a string or number literal
/// or `NULL` as the operand (a client-side prepared statement inlines the
/// parameter) becomes `JSON_COMPACT(<operand>)` (chapter 28, "Queries and
/// expressions": MariaDB has no `JSON` cast; Hibernate's `MySQLDialect`
/// binds every JSON value this way). Other operands are left for R4's
/// rule. `None` when nothing changes.
#[must_use]
pub fn rewrite_json_casts(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let lower_has = sql.windows(4).any(|w| w.eq_ignore_ascii_case(b"json"));
    if !lower_has {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let sig: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && sql[t.start as usize] == c)
    };
    let mut edits: Vec<(usize, usize, usize, usize)> = Vec::new();
    let mut j = 0;
    while j < sig.len() {
        if word(j, b"CAST")
            && punct(j + 1, b'(')
            && sig.get(j + 2).is_some_and(|t| {
                matches!(
                    t.kind,
                    TokenKind::Param
                        | TokenKind::String
                        | TokenKind::NationalString
                        | TokenKind::Number
                ) || (t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(b"NULL"))
            })
            && word(j + 3, b"AS")
            && word(j + 4, b"JSON")
            && punct(j + 5, b')')
        {
            let op = sig[j + 2];
            edits.push((
                sig[j].start as usize,
                sig[j + 5].end as usize,
                op.start as usize,
                op.end as usize,
            ));
            j += 6;
        } else {
            j += 1;
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 8);
    let mut at = 0;
    for (start, end, op_start, op_end) in edits {
        out.extend_from_slice(&sql[at..start]);
        out.extend_from_slice(b"JSON_COMPACT(");
        out.extend_from_slice(&sql[op_start..op_end]);
        out.push(b')');
        at = end;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// MySQL's `INVISIBLE` and `VISIBLE` index options in `CREATE` and
/// `ALTER` statements as MariaDB's `IGNORED` and `NOT IGNORED` (chapter
/// 28, "DDL and types"; R4): after an index's key-part list, and in
/// `ALTER INDEX i INVISIBLE`. A column's `INVISIBLE` (the same word in
/// both servers) is left alone. `None` when nothing changes.
#[must_use]
pub fn rewrite_index_visibility(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql.windows(7).any(|w| w.eq_ignore_ascii_case(b"VISIBLE")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let sig: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && sql[t.start as usize] == c)
    };
    if !(word(0, b"CREATE") || word(0, b"ALTER")) {
        return None;
    }
    let mut edits: Vec<(usize, usize, &str)> = Vec::new();
    let mut depth = 0i32;
    // (paren depth of the index definition, key parts seen)
    let mut index: Option<(i32, bool)> = None;
    let mut j = 0;
    while j < sig.len() {
        if word(j, b"ALTER") && word(j + 1, b"INDEX") {
            let k = j + 3;
            if word(k, b"INVISIBLE") || word(k, b"VISIBLE") {
                let rep = if word(k, b"INVISIBLE") {
                    "IGNORED"
                } else {
                    "NOT IGNORED"
                };
                edits.push((sig[k].start as usize, sig[k].end as usize, rep));
            }
            j = k + 1;
            continue;
        }
        if punct(j, b'(') {
            depth += 1;
        } else if punct(j, b')') {
            depth -= 1;
            if let Some((d, parts)) = index.as_mut() {
                if depth == *d {
                    *parts = true;
                } else if depth < *d {
                    index = None;
                }
            }
        } else if punct(j, b',') || punct(j, b';') {
            if index.is_some_and(|(d, _)| depth <= d) {
                index = None;
            }
        } else if word(j, b"INDEX") || word(j, b"KEY") {
            index = Some((depth, false));
        } else if let Some((d, true)) = index
            && depth == d
            && (word(j, b"INVISIBLE") || word(j, b"VISIBLE"))
        {
            let rep = if word(j, b"INVISIBLE") {
                "IGNORED"
            } else {
                "NOT IGNORED"
            };
            edits.push((sig[j].start as usize, sig[j].end as usize, rep));
        }
        j += 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 8);
    let mut at = 0;
    for (s, e, rep) in edits {
        out.extend_from_slice(&sql[at..s]);
        out.extend_from_slice(rep.as_bytes());
        at = e;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// MySQL's names for unnamed `CHECK` constraints in `CREATE TABLE`
/// (chapter 28, "DDL and types"; R4): `<table>_chk_<n>`, numbered in
/// order of appearance, as MySQL 8.4 names them (MariaDB would say
/// `CONSTRAINT_1`, and 3819 shows the name). A column's `CHECK` moves to
/// the table level under its MySQL name, as MySQL itself stores it
/// (MariaDB refuses a named column check), which also keeps
/// [`errmap`]'s column checks to the JSON checks MariaDB adds. `ALTER
/// TABLE ... ADD CHECK` is left to MariaDB. `None` when nothing changes.
#[must_use]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one pass over the table elements
pub fn name_checks(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql.windows(5).any(|w| w.eq_ignore_ascii_case(b"CHECK")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && sql[tk.start as usize] == c)
    };
    if !word(0, b"CREATE") {
        return None;
    }
    let mut j = 1;
    if word(j, b"OR") && word(j + 1, b"REPLACE") {
        j += 2;
    }
    if word(j, b"TEMPORARY") {
        j += 1;
    }
    if !word(j, b"TABLE") {
        return None;
    }
    j += 1;
    if word(j, b"IF") && word(j + 1, b"NOT") && word(j + 2, b"EXISTS") {
        j += 3;
    }
    // The table name: the last identifier before `(`.
    let mut name = None;
    while j < t.len() && !punct(j, b'(') {
        if t[j].kind.is_ident_like() {
            name = Some(String::from_utf8_lossy(&lex::ident_value(t[j].bytes(sql))).into_owned());
        }
        j += 1;
    }
    let table = name?;
    let open = j;
    if word(open + 1, b"SELECT") || word(open + 1, b"LIKE") {
        return None;
    }
    // Elements at depth 1.
    let mut depth = 0;
    let mut elems: Vec<(usize, usize)> = Vec::new();
    let mut start = open + 1;
    let mut close = None;
    for k in open..t.len() {
        if punct(k, b'(') {
            depth += 1;
        } else if punct(k, b')') {
            depth -= 1;
            if depth == 0 {
                elems.push((start, k));
                close = Some(k);
                break;
            }
        } else if depth == 1 && punct(k, b',') {
            elems.push((start, k));
            start = k + 1;
        }
    }
    let close = close?;
    let matching = |from: usize| -> Option<usize> {
        let mut d = 0;
        for k in from..t.len() {
            if punct(k, b'(') {
                d += 1;
            } else if punct(k, b')') {
                d -= 1;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    };
    let mut n = 0;
    let quoted = |n: u32| format!("`{}_chk_{n}`", table.replace('`', "``"));
    // (byte range to replace, text)
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut moved: Vec<String> = Vec::new();
    for &(a, b) in &elems {
        if a >= b {
            continue;
        }
        if word(a, b"CHECK") {
            n += 1;
            let at = t[a].start as usize;
            edits.push((at, at, format!("CONSTRAINT {} ", quoted(n))));
        } else if word(a, b"CONSTRAINT") {
            if word(a + 1, b"CHECK") {
                n += 1;
                let at = t[a + 1].start as usize;
                edits.push((at, at, format!("{} ", quoted(n))));
            } else if word(a + 2, b"CHECK") {
                // Named: MySQL's counter skips it.
            }
        } else if [
            &b"PRIMARY"[..],
            b"UNIQUE",
            b"KEY",
            b"INDEX",
            b"FOREIGN",
            b"FULLTEXT",
            b"SPATIAL",
            b"PERIOD",
        ]
        .iter()
        .any(|w| word(a, w))
        {
        } else {
            // A column definition: its CHECK clauses move out.
            let mut k = a + 1;
            let mut d = 0;
            while k < b {
                if punct(k, b'(') {
                    d += 1;
                } else if punct(k, b')') {
                    d -= 1;
                } else if d == 0 && word(k, b"CHECK") && punct(k + 1, b'(') {
                    let end = matching(k + 1)?;
                    n += 1;
                    let expr =
                        String::from_utf8_lossy(&sql[t[k + 1].start as usize..t[end].end as usize])
                            .into_owned();
                    edits.push((t[k].start as usize, t[end].end as usize, String::new()));
                    moved.push(format!(", CONSTRAINT {} CHECK {expr}", quoted(n)));
                    k = end + 1;
                    continue;
                }
                k += 1;
            }
        }
    }
    if edits.is_empty() {
        return None;
    }
    if !moved.is_empty() {
        let at = t[close].start as usize;
        edits.push((at, at, moved.concat()));
    }
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 32);
    let mut at = 0;
    for (s, e, rep) in edits {
        out.extend_from_slice(&sql[at..s]);
        out.extend_from_slice(rep.as_bytes());
        at = e;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// MySQL's column attribute `SRID n` in `CREATE` and `ALTER` as
/// MariaDB's `REF_SYSTEM_ID=n` (chapter 28, "DDL and types"; R4; the
/// executable-comment form `/*!80003 SRID n */` is
/// [`lex::versioned`]'s). `None` when nothing changes.
#[must_use]
pub fn rewrite_srid_attribute(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    const GEOMETRY: &[&[u8]] = &[
        b"GEOMETRY",
        b"POINT",
        b"LINESTRING",
        b"POLYGON",
        b"MULTIPOINT",
        b"MULTILINESTRING",
        b"MULTIPOLYGON",
        b"GEOMETRYCOLLECTION",
        b"GEOMCOLLECTION",
    ];
    if !sql.windows(5).any(|w| w.eq_ignore_ascii_case(b" SRID")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let sig: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let first = sig.first()?;
    if first.kind != TokenKind::Word
        || !(first.bytes(sql).eq_ignore_ascii_case(b"CREATE")
            || first.bytes(sql).eq_ignore_ascii_case(b"ALTER"))
    {
        return None;
    }
    // MariaDB takes the attribute only right after the type: it moves
    // there. (insert at, text) and (delete from, to).
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut last_type: Option<usize> = None;
    for (i, t) in sig.iter().enumerate() {
        if t.kind == TokenKind::Word
            && GEOMETRY
                .iter()
                .any(|g| t.bytes(sql).eq_ignore_ascii_case(g))
        {
            last_type = Some(t.end as usize);
        } else if t.kind == TokenKind::Punct && matches!(sql[t.start as usize], b',' | b'(') {
            // A new element (a type may follow `(` of the column list).
        }
        if t.kind == TokenKind::Word
            && t.bytes(sql).eq_ignore_ascii_case(b"SRID")
            && let Some(n) = sig.get(i + 1).filter(|n| n.kind == TokenKind::Number)
        {
            let at = last_type?;
            edits.push((
                at,
                at,
                format!(" REF_SYSTEM_ID={}", String::from_utf8_lossy(n.bytes(sql))),
            ));
            edits.push((t.start as usize, n.end as usize, String::new()));
            last_type = None;
        }
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 16);
    let mut at = 0;
    for (s, e, rep) in edits {
        out.extend_from_slice(&sql[at..s]);
        out.extend_from_slice(rep.as_bytes());
        at = e;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// One text rewrite of [`mysql_text_rewrites`].
type TextStep = fn(&[u8], &LexOptions) -> Option<Vec<u8>>;

/// The text rewrites the MySQL persona applies before name planning:
/// executable comments MariaDB would skip ([`lex::versioned::expand_text_for_mariadb`]),
/// `CAST(? AS JSON)` ([`rewrite_json_casts`]), and from R4 MySQL's names
/// for unnamed checks, `SRID n`, `INVISIBLE` indexes and statement hints. `None` on the MariaDB
/// persona and when nothing changes.
#[must_use]
pub fn mysql_text_rewrites(sql: &[u8], opts: &LexOptions, backend_version: u32) -> Option<Vec<u8>> {
    if opts.persona != sprig_proto_mysql::packets::Persona::MySql84 {
        return None;
    }
    let mut out: Option<Vec<u8>> =
        lex::versioned::expand_text_for_mariadb(sql, opts, backend_version);
    let steps: [TextStep; 5] = [
        rewrite_json_casts,
        name_checks,
        rewrite_srid_attribute,
        // R4: INVISIBLE indexes, and MAX_EXECUTION_TIME and SET_VAR hints
        // as SET STATEMENT.
        rewrite_index_visibility,
        setvar::rewrite_hints,
    ];
    for step in steps {
        if let Some(v) = step(out.as_deref().unwrap_or(sql), opts) {
            out = Some(v);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_get_mysqls_names() {
        let o = LexOptions::mysql84();
        let r = |s: &str| name_checks(s.as_bytes(), &o).map(|v| String::from_utf8(v).unwrap());
        assert_eq!(
            r("CREATE TABLE ck (a INT CHECK (a > 0), b INT, CONSTRAINT named CHECK (b>0), CHECK (b < 100))")
                .as_deref(),
            Some("CREATE TABLE ck (a INT , b INT, CONSTRAINT named CHECK (b>0), CONSTRAINT `ck_chk_2` CHECK (b < 100), CONSTRAINT `ck_chk_1` CHECK (a > 0))")
        );
        assert_eq!(
            r("create table if not exists s.j (id int, d json, constraint check (id > 0))")
                .as_deref(),
            Some(
                "create table if not exists s.j (id int, d json, constraint `j_chk_1` check (id > 0))"
            )
        );
        assert_eq!(r("CREATE TABLE t (a INT)"), None);
        assert_eq!(r("ALTER TABLE t ADD CHECK (a > 0)"), None);
        assert_eq!(r("CREATE TABLE t AS SELECT 1 AS `check`"), None);
    }

    #[test]
    fn srid_attribute() {
        let o = LexOptions::mysql84();
        let r = |s: &str| {
            rewrite_srid_attribute(s.as_bytes(), &o).map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(
            r("CREATE TABLE s (loc POINT NOT NULL SRID 4326, SPATIAL INDEX (loc))").as_deref(),
            Some("CREATE TABLE s (loc POINT REF_SYSTEM_ID=4326 NOT NULL , SPATIAL INDEX (loc))")
        );
        assert_eq!(r("SELECT ST_SRID(loc) FROM s"), None);
        // The executable-comment form after NOT NULL (PyMySQL's issue 363
        // test, mysqldump 8.4): MariaDB refuses REF_SYSTEM_ID there.
        let all = mysql_text_rewrites(
            b"CREATE TABLE i (geom LINESTRING NOT NULL /*!80003 SRID 0 */, SPATIAL KEY geom (geom))",
            &o,
            110_809,
        )
        .map(|v| String::from_utf8(v).unwrap());
        assert_eq!(
            all.as_deref(),
            Some(
                "CREATE TABLE i (geom LINESTRING REF_SYSTEM_ID=0 NOT NULL , SPATIAL KEY geom (geom))"
            )
        );
    }

    #[test]
    fn index_visibility() {
        let o = LexOptions::mysql84();
        let r = |s: &str| {
            rewrite_index_visibility(s.as_bytes(), &o).map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(
            r("CREATE INDEX k ON t (a, b(10)) INVISIBLE").as_deref(),
            Some("CREATE INDEX k ON t (a, b(10)) IGNORED")
        );
        assert_eq!(
            r("CREATE TABLE t (a varchar(10) INVISIBLE, KEY k (a) COMMENT 'x' INVISIBLE, b int)")
                .as_deref(),
            Some("CREATE TABLE t (a varchar(10) INVISIBLE, KEY k (a) COMMENT 'x' IGNORED, b int)")
        );
        assert_eq!(
            r("ALTER TABLE t ALTER INDEX k VISIBLE, ADD INDEX j (b) INVISIBLE").as_deref(),
            Some("ALTER TABLE t ALTER INDEX k NOT IGNORED, ADD INDEX j (b) IGNORED")
        );
        assert_eq!(r("ALTER TABLE t ADD COLUMN c int INVISIBLE"), None);
        assert_eq!(r("SELECT 'INVISIBLE'"), None);
    }

    #[test]
    fn json_casts_of_parameters_and_strings() {
        let o = LexOptions::mysql84();
        let r = |s: &str| {
            mysql_text_rewrites(s.as_bytes(), &o, 999_999).map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(
            r("insert into t (a,j,b) values (?,cast(? as json),?)").as_deref(),
            Some("insert into t (a,j,b) values (?,JSON_COMPACT(?),?)")
        );
        assert_eq!(
            r("SELECT CAST('[1, 2]' AS JSON), CAST(c AS JSON)").as_deref(),
            Some("SELECT JSON_COMPACT('[1, 2]'), CAST(c AS JSON)")
        );
        assert_eq!(r("SELECT 'CAST(? AS JSON)'"), None);
        assert_eq!(
            r("values (cast(null as json), CAST(1 AS JSON))").as_deref(),
            Some("values (JSON_COMPACT(null), JSON_COMPACT(1))")
        );
        assert_eq!(
            r("CREATE TABLE t (k int /*!80023 INVISIBLE */, j json DEFAULT (CAST('{}' AS JSON)))")
                .as_deref(),
            Some("CREATE TABLE t (k int INVISIBLE, j json DEFAULT (JSON_COMPACT('{}')))")
        );
        assert_eq!(
            mysql_text_rewrites(b"SELECT CAST(? AS JSON)", &LexOptions::mariadb(), 999_999),
            None
        );
    }
}
