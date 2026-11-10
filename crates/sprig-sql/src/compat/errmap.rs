//! Error mapping on port 3306 (chapter 28, "Error mapping"; build step 5).
//!
//! A MariaDB error reaches a MySQL persona client with MySQL 8.4's number,
//! SQLSTATE and wording where the two servers differ. Names in the message
//! are already logical (chapter 07). The MariaDB persona passes errors
//! unchanged.
//!
//! | MariaDB | When | MySQL 8.4 |
//! |---|---|---|
//! | 1969 `max_statement_time` exceeded | always | 3024 (`HY000`) |
//! | 4025 `CONSTRAINT ... failed` | a column-level check (`t.c`): the JSON check of a `JSON` column | 3140 (`22032`) |
//! | 4025 | a table check | 3819 (`HY000`) |
//! | 1451 without the row detail | `DROP TABLE` of a referenced table | 3730 (`HY000`) |
//! | 1205 lock wait timeout | the statement had `NOWAIT` (the relay knows) | 3572 |
//! | any message naming "MariaDB" | `... your MariaDB server version ...` | "MySQL" |
//!
//! R4 adds MySQL 8.4's wording where MariaDB 11.8's differs ([`reworded`],
//! every row captured from both servers on 9 November 2026), and MySQL's
//! key names in 1062 (`'t.PRIMARY'`), which need the statement's target
//! table ([`dup_key_table`], [`with_dup_key_table`]):
//!
//! | MariaDB 11.8 | MySQL 8.4 |
//! |---|---|
//! | 1054, 1052 `... in 'SELECT'`, `'WHERE'`, `'ORDER BY'`, ... | `'field list'`, `'where clause'`, `'order clause'`, ... |
//! | 1366, 1292, 1265 `... for column `db`.`t`.`c` at row n` (1366: `22007`) | `... for column 'c' at row n` (1366: `HY000`) |
//! | 1091 `Can't DROP COLUMN `c`; check that it exists` | `Can't DROP 'c'; check that column/key exists` |
//! | 4092 `Unknown VIEW: 'db.v'` | 1051 `Unknown table 'db.v'` |
//! | 1347 `'db.t' is not of type 'VIEW'` | `'db.t' is not VIEW` |
//! | 1906 generated column value ignored | 3105 `... is not allowed.` |
//! | 4078 `Illegal parameter data types int and row ...` | 1241 `Operand should contain 1 column(s)` |
//!
//! Still MariaDB's: 3140's parser detail and position (MariaDB doesn't say
//! why the text isn't JSON), 3730's table and constraint names (MariaDB
//! doesn't give them), 1690's expression text, and the JSON function
//! errors MySQL raises where MariaDB returns `NULL` with a warning (3141,
//! 3143).

use sprig_common::{ErrorKind, SprigError};
use sprig_proto_mysql::packets::Persona;

/// An error as the wire carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// The error number.
    pub code: u16,
    /// The SQLSTATE.
    pub sqlstate: [u8; 5],
    /// The message.
    pub message: String,
}

impl WireError {
    /// An error.
    #[must_use]
    pub fn new(code: u16, sqlstate: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            sqlstate,
            message: message.into(),
        }
    }
}

/// The error a client of `persona` gets for `e`, when it differs: on 3306,
/// a backend (MariaDB) error mapped by [`to_mysql`]; SprigDB's own and
/// standard errors are already MySQL's. `None`: send `e` as it is.
#[must_use]
pub fn for_persona(persona: Persona, e: &SprigError) -> Option<SprigError> {
    if persona != Persona::MySql84 {
        return None;
    }
    let ErrorKind::Backend { errno, sqlstate } = e.kind else {
        // A MariaDB error the gateway re-raised as its own (the DDL path
        // does): only MariaDB's wording changes.
        if let ErrorKind::Standard { errno, sqlstate } = e.kind {
            let m = reworded(&WireError::new(errno, sqlstate, e.message.clone()))?;
            let mut out = SprigError::standard(m.code, m.sqlstate, m.message);
            out.retryable = e.retryable;
            return Some(out);
        }
        return None;
    };
    let m = to_mysql(&WireError::new(errno, sqlstate, e.message.clone()))?;
    let mut out = SprigError::backend(m.code, m.sqlstate, m.message);
    out.retryable = e.retryable;
    Some(out)
}

/// The first backquoted name in `s`, unescaped.
fn first_backquoted(s: &str) -> Option<String> {
    let start = s.find('`')? + 1;
    let mut out = String::new();
    let b: Vec<char> = s[start..].chars().collect();
    let mut i = 0;
    while i < b.len() {
        if b[i] == '`' {
            if b.get(i + 1) == Some(&'`') {
                out.push('`');
                i += 2;
                continue;
            }
            return Some(out);
        }
        out.push(b[i]);
        i += 1;
    }
    None
}

/// The MySQL 8.4 form of a MariaDB backend error, for the MySQL persona.
/// `None` when the error passes unchanged.
#[must_use]
pub fn to_mysql(e: &WireError) -> Option<WireError> {
    let mapped = match e.code {
        1969 => Some(WireError::new(
            3024,
            *b"HY000",
            "Query execution was interrupted, maximum statement execution time exceeded",
        )),
        4025 if e.message.starts_with("CONSTRAINT ") => {
            let name = first_backquoted(&e.message)?;
            if name.contains('.') {
                Some(WireError::new(
                    3140,
                    *b"22032",
                    format!(
                        "Invalid JSON text: \"Invalid value.\" at position 0 in value for column '{name}'."
                    ),
                ))
            } else {
                Some(WireError::new(
                    3819,
                    *b"HY000",
                    format!("Check constraint '{name}' is violated."),
                ))
            }
        }
        1451 if e.message.trim_end()
            == "Cannot delete or update a parent row: a foreign key constraint fails" =>
        {
            Some(WireError::new(
                3730,
                *b"HY000",
                "Cannot drop table referenced by a foreign key constraint.",
            ))
        }
        _ => reworded(e),
    };
    if mapped.is_some() {
        return mapped;
    }
    e.message.contains("MariaDB").then(|| WireError {
        code: e.code,
        sqlstate: e.sqlstate,
        message: e.message.replace("MariaDB", "MySQL"),
    })
}

/// MariaDB 11.8's clause names in 1054 and 1052, and MySQL 8.4's.
const CLAUSES: &[(&str, &str)] = &[
    ("SELECT", "field list"),
    ("SET", "field list"),
    ("INSERT INTO", "field list"),
    ("UPDATE", "field list"),
    ("VALUES", "field list"),
    ("RETURNING", "field list"),
    ("WHERE", "where clause"),
    ("GROUP BY", "group statement"),
    ("HAVING", "having clause"),
    ("ORDER BY", "order clause"),
    ("ON", "on clause"),
    ("PARTITION BY", "window partition by"),
];

/// `` `db`.`t`.`c` `` (MariaDB's column naming in value errors) as `'c'`.
fn mysql_column_names(msg: &str) -> Option<String> {
    let at = msg.find("for column `")? + "for column ".len();
    let rest = &msg[at..];
    // Up to three backquoted parts separated by dots.
    let mut parts = Vec::new();
    let mut i = 0;
    let b = rest.as_bytes();
    loop {
        if b.get(i) != Some(&b'`') {
            return None;
        }
        let mut name = String::new();
        i += 1;
        loop {
            match b.get(i) {
                Some(b'`') if b.get(i + 1) == Some(&b'`') => {
                    name.push('`');
                    i += 2;
                }
                Some(b'`') => {
                    i += 1;
                    break;
                }
                Some(_) => {
                    let ch = rest[i..].chars().next()?;
                    name.push(ch);
                    i += ch.len_utf8();
                }
                None => return None,
            }
        }
        parts.push(name);
        if b.get(i) == Some(&b'.') {
            i += 1;
        } else {
            break;
        }
    }
    let col = parts.last()?;
    Some(format!("{}'{}'{}", &msg[..at], col, &rest[i..]))
}

/// MySQL 8.4's wording of a MariaDB 11.8 error where the two differ only
/// in text (or number and text), as the module table lists. `None` when
/// it is already MySQL's.
#[must_use]
pub fn reworded(e: &WireError) -> Option<WireError> {
    let m = e.message.as_str();
    match e.code {
        1054 | 1052 => {
            for (maria, mysql) in CLAUSES {
                let from = format!(" in '{maria}'");
                if let Some(at) = m.rfind(&from) {
                    let mut msg = m.to_owned();
                    msg.replace_range(at..at + from.len(), &format!(" in '{mysql}'"));
                    return Some(WireError::new(e.code, e.sqlstate, msg));
                }
                // 1052: "Column 'id' in WHERE is ambiguous".
                let from = format!(" in {maria} is ambiguous");
                if let Some(at) = m.rfind(&from) {
                    let mut msg = m.to_owned();
                    msg.replace_range(at..at + from.len(), &format!(" in {mysql} is ambiguous"));
                    return Some(WireError::new(e.code, e.sqlstate, msg));
                }
            }
            if let Some(at) = m.rfind(" in 'CHECK'") {
                return Some(WireError::new(
                    e.code,
                    e.sqlstate,
                    format!("{} in 'check constraint expression'", &m[..at]),
                ));
            }
            None
        }
        1366 | 1292 | 1265 | 1264 | 1406 | 1048 => {
            // MariaDB numbers the row of a statement inside stored code 0;
            // MySQL counts from 1 (MySQL Connector/J's suite checks it).
            let named = mysql_column_names(m);
            let text = named.as_deref().unwrap_or(m);
            let msg = match text.strip_suffix(" at row 0") {
                Some(head) => format!("{head} at row 1"),
                None => named?,
            };
            let state = if e.code == 1366 {
                *b"HY000"
            } else {
                e.sqlstate
            };
            Some(WireError::new(e.code, state, msg))
        }
        1091 => {
            // "Can't DROP COLUMN `c`; check that it exists"
            let rest = m.strip_prefix("Can't DROP ")?;
            let tick = rest.find('`')?;
            let name = first_backquoted(&rest[tick..])?;
            Some(WireError::new(
                1091,
                e.sqlstate,
                format!("Can't DROP '{name}'; check that column/key exists"),
            ))
        }
        4092 => {
            let name = m.strip_prefix("Unknown VIEW: ")?;
            Some(WireError::new(
                1051,
                *b"42S02",
                format!("Unknown table {name}"),
            ))
        }
        1347 => m
            .strip_suffix(" is not of type 'VIEW'")
            .map(|n| WireError::new(1347, e.sqlstate, format!("{n} is not VIEW"))),
        1906 => m
            .strip_suffix(" has been ignored")
            .map(|head| WireError::new(3105, *b"HY000", format!("{head} is not allowed."))),
        4078 if m.contains(" and row ") || m.contains("types row and ") => Some(WireError::new(
            1241,
            *b"21000",
            "Operand should contain 1 column(s)",
        )),
        _ => None,
    }
}

/// The table a `1062` names on the MySQL persona: the target of a
/// single-table `INSERT`, `REPLACE`, `UPDATE` or `LOAD DATA` (its own
/// name, without the database). `None` for other statements and
/// multi-table updates.
#[must_use]
pub fn dup_key_table(sql: &[u8], opts: &crate::lex::LexOptions) -> Option<String> {
    use crate::lex::{self, TokenKind};
    let head = sql.trim_ascii_start();
    let starts = |w: &[u8]| head.len() >= w.len() && head[..w.len()].eq_ignore_ascii_case(w);
    if !(starts(b"INSERT") || starts(b"REPLACE") || starts(b"UPDATE") || starts(b"LOAD")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let is = |j: usize, w: &str| {
        t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    let mut j;
    if is(0, "LOAD") {
        j = t.iter().position(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(b"TABLE")
        })? + 1;
    } else {
        j = 1;
        while ["LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO"]
            .iter()
            .any(|w| is(j, w))
        {
            j += 1;
        }
    }
    let name_at = |j: usize| -> Option<String> {
        let tk = t.get(j)?;
        tk.kind
            .is_ident_like()
            .then(|| String::from_utf8_lossy(&lex::ident_value(tk.bytes(sql))).into_owned())
    };
    let mut name = name_at(j)?;
    if t.get(j + 1)
        .is_some_and(|tk| tk.kind == TokenKind::Punct && tk.bytes(sql) == b".")
    {
        name = name_at(j + 2)?;
        j += 2;
    }
    // A single-table UPDATE sets right after its table (or alias).
    if is(0, "UPDATE") {
        let next = j + 1;
        let ok = is(next, "SET")
            || (is(next, "AS") && is(next + 2, "SET"))
            || (t.get(next).is_some_and(|tk| tk.kind.is_ident_like()) && is(next + 1, "SET"));
        if !ok {
            return None;
        }
    }
    Some(name)
}

/// A `1062`'s message with MySQL's `'table.key'` form, given the target
/// table; unchanged when the key name already has a table.
#[must_use]
pub fn dup_key_message(message: &str, table: &str) -> Option<String> {
    let at = message.rfind(" for key '")? + " for key '".len();
    let key = message[at..].strip_suffix('\'')?;
    if key.contains('.') {
        return None;
    }
    Some(format!("{}{table}.{key}'", &message[..at]))
}

/// [`dup_key_message`] applied to an error, any kind: a backend 1062 or
/// the router's own (chapter 19's lookup tables name the user's key).
#[must_use]
pub fn with_dup_key_table(e: &SprigError, table: &str) -> Option<SprigError> {
    if e.protocol().mysql_errno != 1062 {
        return None;
    }
    let message = dup_key_message(&e.message, table)?;
    Some(SprigError {
        kind: e.kind,
        message,
        retryable: e.retryable,
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::trivially_copy_pass_by_ref)] // reads as b"23000"
    fn map(code: u16, state: &[u8; 5], msg: &str) -> Option<(u16, [u8; 5], String)> {
        to_mysql(&WireError::new(code, *state, msg)).map(|e| (e.code, e.sqlstate, e.message))
    }

    #[test]
    fn mariadb_only_codes_become_mysqls() {
        assert_eq!(
            map(
                1969,
                b"70100",
                "Query execution was interrupted (max_statement_time exceeded)"
            ),
            Some((
                3024,
                *b"HY000",
                "Query execution was interrupted, maximum statement execution time exceeded"
                    .to_owned()
            ))
        );
        // Captured from MariaDB 11.8 and MySQL 8.4 on 8 November 2026.
        assert_eq!(
            map(4025, b"23000", "CONSTRAINT `j.k` failed for `shop`.`j`"),
            Some((
                3140,
                *b"22032",
                "Invalid JSON text: \"Invalid value.\" at position 0 in value for column 'j.k'."
                    .to_owned()
            ))
        );
        assert_eq!(
            map(4025, b"23000", "CONSTRAINT `c_chk_1` failed for `shop`.`c`"),
            Some((
                3819,
                *b"HY000",
                "Check constraint 'c_chk_1' is violated.".to_owned()
            ))
        );
        assert_eq!(
            map(
                1451,
                b"23000",
                "Cannot delete or update a parent row: a foreign key constraint fails"
            )
            .map(|m| m.0),
            Some(3730)
        );
        // A DELETE's 1451 carries the row detail and stays.
        assert_eq!(
            map(
                1451,
                b"23000",
                "Cannot delete or update a parent row: a foreign key constraint fails (`shop`.`c`, CONSTRAINT `fk` FOREIGN KEY (`p`) REFERENCES `j` (`id`))"
            ),
            None
        );
    }

    #[test]
    fn only_backend_errors_on_3306_change() {
        let e = SprigError::backend(1969, *b"70100", "Query execution was interrupted");
        let m = for_persona(Persona::MySql84, &e).unwrap();
        assert_eq!(m.protocol().mysql_errno, 3024);
        assert!(for_persona(Persona::MariaDb, &e).is_none());
        let s = SprigError::standard(1969, *b"70100", "x");
        assert!(for_persona(Persona::MySql84, &s).is_none());
    }

    #[test]
    fn mariadb_in_messages_reads_mysql() {
        let m = map(
            1064,
            b"42000",
            "You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near 'x' at line 1",
        )
        .unwrap();
        assert_eq!(m.0, 1064);
        assert!(m.2.contains("your MySQL server version"));
        assert_eq!(map(1146, b"42S02", "Table 'shop.t' doesn't exist"), None);
    }

    // Captured from MySQL 8.4.11 and MariaDB 11.8.9 on 9 November 2026.
    #[test]
    fn mysql_wording() {
        let w = |code: u16, msg: &str| map(code, b"42S22", msg).map(|m| (m.0, m.2));
        assert_eq!(
            w(1054, "Unknown column 'nocol' in 'SELECT'"),
            Some((1054, "Unknown column 'nocol' in 'field list'".to_owned()))
        );
        assert_eq!(
            w(1054, "Unknown column 'p.nocol' in 'ON'").unwrap().1,
            "Unknown column 'p.nocol' in 'on clause'"
        );
        assert_eq!(
            w(1054, "Unknown column 'nocol' in 'GROUP BY'").unwrap().1,
            "Unknown column 'nocol' in 'group statement'"
        );
        assert_eq!(
            w(1052, "Column 'id' in WHERE is ambiguous").unwrap().1,
            "Column 'id' in where clause is ambiguous"
        );
        assert_eq!(
            map(
                1366,
                b"22007",
                "Incorrect integer value: 'x' for column `shop`.`p`.`id` at row 1"
            ),
            Some((
                1366,
                *b"HY000",
                "Incorrect integer value: 'x' for column 'id' at row 1".to_owned()
            ))
        );
        assert_eq!(
            map(
                1292,
                b"22007",
                "Incorrect date value: '2020-13-45' for column `r`.`p`.`d` at row 1"
            )
            .unwrap()
            .2,
            "Incorrect date value: '2020-13-45' for column 'd' at row 1"
        );
        assert_eq!(
            map(
                1091,
                b"42000",
                "Can't DROP INDEX `nokey`; check that it exists"
            )
            .unwrap()
            .2,
            "Can't DROP 'nokey'; check that column/key exists"
        );
        assert_eq!(
            map(4092, b"42S02", "Unknown VIEW: 'shop.v'"),
            Some((1051, *b"42S02", "Unknown table 'shop.v'".to_owned()))
        );
        assert_eq!(
            map(1347, b"HY000", "'shop.p' is not of type 'VIEW'")
                .unwrap()
                .2,
            "'shop.p' is not VIEW"
        );
        assert_eq!(
            map(
                1906,
                b"HY000",
                "The value specified for generated column 'b' in table 'g' has been ignored"
            ),
            Some((
                3105,
                *b"HY000",
                "The value specified for generated column 'b' in table 'g' is not allowed."
                    .to_owned()
            ))
        );
        assert_eq!(
            map(
                4078,
                b"HY000",
                "Illegal parameter data types int and row for operation '='"
            )
            .map(|m| m.0),
            Some(1241)
        );
        assert_eq!(
            map(1054, b"42S22", "Unknown column 'x' in 'field list'"),
            None
        );
    }

    #[test]
    fn duplicate_keys_name_the_table() {
        let o = crate::lex::LexOptions::mysql84();
        let t = |s: &str| dup_key_table(s.as_bytes(), &o);
        assert_eq!(t("INSERT INTO p VALUES (1)").as_deref(), Some("p"));
        assert_eq!(
            t("insert ignore into shop.`Orders` (a) values (1)").as_deref(),
            Some("Orders")
        );
        assert_eq!(t("REPLACE p SET a = 1").as_deref(), Some("p"));
        assert_eq!(t("UPDATE p SET u = 'a' WHERE id = 2").as_deref(), Some("p"));
        assert_eq!(t("UPDATE p AS x SET u = 'a'").as_deref(), Some("p"));
        assert_eq!(t("UPDATE p JOIN c ON c.id = p.id SET p.u = 'a'"), None);
        assert_eq!(
            t("LOAD DATA LOCAL INFILE 'f' INTO TABLE t2 FIELDS TERMINATED BY ','").as_deref(),
            Some("t2")
        );
        assert_eq!(t("SELECT 1"), None);
        assert_eq!(
            dup_key_message("Duplicate entry '1' for key 'PRIMARY'", "p").as_deref(),
            Some("Duplicate entry '1' for key 'p.PRIMARY'")
        );
        assert_eq!(
            dup_key_message("Duplicate entry 'a' for key 'p.uk_u'", "p"),
            None
        );
        let e = SprigError::backend(1062, *b"23000", "Duplicate entry 'a' for key 'uk_u'");
        assert_eq!(
            with_dup_key_table(&e, "p").unwrap().message,
            "Duplicate entry 'a' for key 'p.uk_u'"
        );
    }

    #[test]
    fn rows_count_from_one() {
        let e = WireError::new(
            1366,
            *b"22007",
            "Incorrect integer value: 'eight' for column `db`.`t`.`n` at row 0".to_owned(),
        );
        assert_eq!(
            reworded(&e).unwrap().message,
            "Incorrect integer value: 'eight' for column 'n' at row 1"
        );
        let e = WireError::new(
            1366,
            *b"HY000",
            "Incorrect integer value: 'x' for column 'n' at row 0".to_owned(),
        );
        assert_eq!(
            reworded(&e).unwrap().message,
            "Incorrect integer value: 'x' for column 'n' at row 1"
        );
    }
}
