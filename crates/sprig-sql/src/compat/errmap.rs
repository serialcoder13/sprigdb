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
//! Not yet (R4): MySQL's key names in 1062 (`'t.PRIMARY'`, which needs the
//! statement's target table), the messages of the 60 commonest codes from
//! MySQL's templates, and 3140's parser detail and position (MariaDB
//! doesn't say why the text isn't JSON).

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
    let ErrorKind::Backend { errno, sqlstate } = e.kind else {
        return None;
    };
    if persona != Persona::MySql84 {
        return None;
    }
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
        _ => None,
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
}
