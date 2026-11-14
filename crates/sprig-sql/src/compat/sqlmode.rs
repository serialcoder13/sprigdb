//! `sql_mode` on the MySQL persona.
//!
//! A 3306 session has two values:
//!
//! - **The client's view**, what `@@sql_mode` and `SHOW VARIABLES` answer:
//!   MySQL 8.4's spelling and member lists, `ONLY_FULL_GROUP_BY` and
//!   `TIME_TRUNCATE_FRACTIONAL` included when set.
//! - **The backend value**, what the session's MariaDB connections run
//!   with ([`Mode::backend`]): the members only (no `ANSI` or
//!   `TRADITIONAL` marker, whose MariaDB expansions differ),
//!   `ONLY_FULL_GROUP_BY` removed (MariaDB applies it without functional
//!   dependencies and rejects queries MySQL accepts), and
//!   `TIME_ROUND_FRACTIONAL` added unless the client asked for
//!   `TIME_TRUNCATE_FRACTIONAL` (MySQL rounds fractional seconds, MariaDB
//!   truncates).
//!
//! Values are parsed as MySQL 8.4 does ([`parse_mysql`]): names in any
//! case, empty items skipped, any other item (modes MySQL 8.0 removed
//! such as `MAXDB`, stray spaces) fails with 1231. The MariaDB-only names
//! SprigDB's own dumps can carry (`NO_AUTO_CREATE_USER`, ...) are accepted
//! and ignored. A number is the set of MySQL's bits.

/// One mode: its MySQL 8.4 bit (the display order), its name, and whether
/// MariaDB knows it.
struct Def {
    bit: u64,
    name: &'static str,
}

const ONLY_FULL_GROUP_BY: u64 = 1 << 5;
const ANSI: u64 = 1 << 18;
const TRADITIONAL: u64 = 1 << 27;
const TIME_TRUNCATE_FRACTIONAL: u64 = 1 << 32;

/// MySQL 8.4's modes, in its bit (and display) order.
const MODES: &[Def] = &[
    Def {
        bit: 1,
        name: "REAL_AS_FLOAT",
    },
    Def {
        bit: 1 << 1,
        name: "PIPES_AS_CONCAT",
    },
    Def {
        bit: 1 << 2,
        name: "ANSI_QUOTES",
    },
    Def {
        bit: 1 << 3,
        name: "IGNORE_SPACE",
    },
    Def {
        bit: 1 << 4,
        name: "NOT_USED",
    },
    Def {
        bit: ONLY_FULL_GROUP_BY,
        name: "ONLY_FULL_GROUP_BY",
    },
    Def {
        bit: 1 << 6,
        name: "NO_UNSIGNED_SUBTRACTION",
    },
    Def {
        bit: 1 << 7,
        name: "NO_DIR_IN_CREATE",
    },
    Def {
        bit: 1 << 8,
        name: "NOT_USED_9",
    },
    Def {
        bit: 1 << 9,
        name: "NOT_USED_10",
    },
    Def {
        bit: 1 << 10,
        name: "NOT_USED_11",
    },
    Def {
        bit: 1 << 11,
        name: "NOT_USED_12",
    },
    Def {
        bit: 1 << 12,
        name: "NOT_USED_13",
    },
    Def {
        bit: 1 << 13,
        name: "NOT_USED_14",
    },
    Def {
        bit: 1 << 14,
        name: "NOT_USED_15",
    },
    Def {
        bit: 1 << 15,
        name: "NOT_USED_16",
    },
    Def {
        bit: 1 << 16,
        name: "NOT_USED_17",
    },
    Def {
        bit: 1 << 17,
        name: "NOT_USED_18",
    },
    Def {
        bit: ANSI,
        name: "ANSI",
    },
    Def {
        bit: 1 << 19,
        name: "NO_AUTO_VALUE_ON_ZERO",
    },
    Def {
        bit: 1 << 20,
        name: "NO_BACKSLASH_ESCAPES",
    },
    Def {
        bit: 1 << 21,
        name: "STRICT_TRANS_TABLES",
    },
    Def {
        bit: 1 << 22,
        name: "STRICT_ALL_TABLES",
    },
    Def {
        bit: 1 << 23,
        name: "NO_ZERO_IN_DATE",
    },
    Def {
        bit: 1 << 24,
        name: "NO_ZERO_DATE",
    },
    Def {
        bit: 1 << 25,
        name: "ALLOW_INVALID_DATES",
    },
    Def {
        bit: 1 << 26,
        name: "ERROR_FOR_DIVISION_BY_ZERO",
    },
    Def {
        bit: TRADITIONAL,
        name: "TRADITIONAL",
    },
    Def {
        bit: 1 << 28,
        name: "NOT_USED_29",
    },
    Def {
        bit: 1 << 29,
        name: "HIGH_NOT_PRECEDENCE",
    },
    Def {
        bit: 1 << 30,
        name: "NO_ENGINE_SUBSTITUTION",
    },
    Def {
        bit: 1 << 31,
        name: "PAD_CHAR_TO_FULL_LENGTH",
    },
    Def {
        bit: TIME_TRUNCATE_FRACTIONAL,
        name: "TIME_TRUNCATE_FRACTIONAL",
    },
];

/// Bits no name may set by hand (`NOT_USED*`): MySQL accepts the names,
/// shows them, and they do nothing. MariaDB doesn't know them.
const NOT_USED: u64 = (1 << 4) | (0x3ff << 8) | (1 << 28);

/// The members `ANSI` adds (MySQL 8.4).
const ANSI_MEMBERS: u64 = 1 | (1 << 1) | (1 << 2) | (1 << 3) | ONLY_FULL_GROUP_BY;
/// The members `TRADITIONAL` adds (MySQL 8.4).
const TRADITIONAL_MEMBERS: u64 =
    (1 << 21) | (1 << 22) | (1 << 23) | (1 << 24) | (1 << 26) | (1 << 30);

/// MySQL 8.4's default.
pub const MYSQL84_DEFAULT: Mode =
    Mode(ONLY_FULL_GROUP_BY | (1 << 21) | (1 << 23) | (1 << 24) | (1 << 26) | (1 << 30));

/// A MySQL 8.4 `sql_mode` value: its bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mode(pub u64);

/// `Variable 'sql_mode' can't be set to the value of 'x'` (1231, `42000`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadValue(pub String);

impl BadValue {
    /// MySQL's message.
    #[must_use]
    pub fn message(&self) -> String {
        format!(
            "Variable 'sql_mode' can't be set to the value of '{}'",
            self.0
        )
    }
}

fn by_name(name: &str) -> Option<u64> {
    MODES
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
        .map(|d| d.bit)
}

/// Adds the members of `ANSI` and `TRADITIONAL` when set.
const fn expand(bits: u64) -> u64 {
    let mut b = bits;
    if b & ANSI != 0 {
        b |= ANSI_MEMBERS;
    }
    if b & TRADITIONAL != 0 {
        b |= TRADITIONAL_MEMBERS;
    }
    b
}

/// MariaDB-only modes that SprigDB's own output can carry: stored programs keep
/// the `sql_mode` MariaDB recorded, and `mysqldump` through 3306 writes it back
/// as `SET sql_mode = '...,NO_AUTO_CREATE_USER,...'` (measured). They are
/// accepted and ignored, so a dump restores; MySQL 8.4 itself would refuse them
/// with 1231.
const MARIADB_REPORTED: &[&str] = &[
    "NO_AUTO_CREATE_USER",
    "TIME_ROUND_FRACTIONAL",
    "EMPTY_STRING_IS_NULL",
    "SIMULTANEOUS_ASSIGNMENT",
    "IGNORE_BAD_TABLE_OPTIONS",
];

/// Parses a value the MySQL way: comma-separated names, any case, empty
/// items skipped. The first item MySQL 8.4 doesn't know is the error.
///
/// # Errors
/// [`BadValue`] naming that item.
pub fn parse_mysql(text: &str) -> Result<Mode, BadValue> {
    let mut bits = 0;
    for item in text.split(',') {
        if item.is_empty()
            || MARIADB_REPORTED
                .iter()
                .any(|m| m.eq_ignore_ascii_case(item))
        {
            continue;
        }
        match by_name(item) {
            Some(b) => bits |= b,
            None => return Err(BadValue(item.to_owned())),
        }
    }
    Ok(Mode(expand(bits)))
}

/// A numeric value: MySQL's bits.
///
/// # Errors
/// [`BadValue`] for bits MySQL 8.4 doesn't define.
pub fn from_number(n: u64) -> Result<Mode, BadValue> {
    let all = MODES.iter().fold(0, |a, d| a | d.bit);
    if n & !all != 0 {
        return Err(BadValue(n.to_string()));
    }
    Ok(Mode(expand(n)))
}

/// The client's view of a value MariaDB reports (the backend value after
/// a statement set it from an expression, such as `SET sql_mode =
/// @old_mode`): MySQL's names kept, MariaDB-only names dropped
/// (`TIME_ROUND_FRACTIONAL` is the gateway's own; `NO_AUTO_CREATE_USER`,
/// `EMPTY_STRING_IS_NULL`, ... have no MySQL meaning).
#[must_use]
pub fn from_mariadb(text: &str) -> Mode {
    let bits = text
        .split(',')
        .filter_map(|i| by_name(i.trim()))
        .fold(0, |a, b| a | b)
        & !NOT_USED;
    Mode(expand(bits))
}

impl Mode {
    /// MySQL 8.4's rendering, in its bit order.
    #[must_use]
    pub fn render(self) -> String {
        MODES
            .iter()
            .filter(|d| self.0 & d.bit != 0)
            .map(|d| d.name)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Whether `ONLY_FULL_GROUP_BY` is on (the gateway's check; not
    /// sent to MariaDB).
    #[must_use]
    pub const fn only_full_group_by(self) -> bool {
        self.0 & ONLY_FULL_GROUP_BY != 0
    }

    /// The value the backend connections run with, rendered in MariaDB's
    /// order (which agrees with MySQL's for the names both have), so a
    /// value MariaDB reports back compares equal.
    #[must_use]
    pub fn backend(self) -> String {
        let keep = self.0
            & !(ONLY_FULL_GROUP_BY | ANSI | TRADITIONAL | TIME_TRUNCATE_FRACTIONAL | NOT_USED);
        let mut names: Vec<&str> = MODES
            .iter()
            .filter(|d| keep & d.bit != 0)
            .map(|d| d.name)
            .collect();
        if self.0 & TIME_TRUNCATE_FRACTIONAL == 0 {
            names.push("TIME_ROUND_FRACTIONAL");
        }
        names.join(",")
    }
}

/// The MariaDB-only name that marks `ONLY_FULL_GROUP_BY` in a stored
/// program's recorded `sql_mode` (see [`Mode::stored_program`]). MariaDB
/// 11.8 gives it no meaning inside stored code: it only stops `GRANT` from
/// creating accounts, which tenants' code can't do.
pub const ONLY_FULL_GROUP_BY_MARK: &str = "NO_AUTO_CREATE_USER";

impl Mode {
    /// The mode a stored program created by a 3306 session records: the
    /// session's backend value, plus [`ONLY_FULL_GROUP_BY_MARK`] when the
    /// client's view has `ONLY_FULL_GROUP_BY` (which MariaDB doesn't run),
    /// so `SHOW CREATE` shows it again ([`stored_program_view`]).
    #[must_use]
    pub fn stored_program(self) -> String {
        let mut b = self.backend();
        if self.only_full_group_by() {
            if !b.is_empty() {
                b.push(',');
            }
            b.push_str(ONLY_FULL_GROUP_BY_MARK);
        }
        b
    }
}

/// What 3306 shows as a stored program's `sql_mode` (`SHOW CREATE
/// PROCEDURE`, `information_schema.ROUTINES.SQL_MODE`, ...), from the
/// value MariaDB recorded: MySQL's names, `ONLY_FULL_GROUP_BY` where
/// [`ONLY_FULL_GROUP_BY_MARK`] is, and `TIME_TRUNCATE_FRACTIONAL` where
/// `TIME_ROUND_FRACTIONAL` is missing (MariaDB truncates there).
#[must_use]
pub fn stored_program_view(recorded: &str) -> String {
    let has = |n: &str| {
        recorded
            .split(',')
            .any(|i| i.trim().eq_ignore_ascii_case(n))
    };
    let mut m = from_mariadb(recorded);
    if has(ONLY_FULL_GROUP_BY_MARK) {
        m.0 |= ONLY_FULL_GROUP_BY;
    }
    if !has("TIME_ROUND_FRACTIONAL") {
        m.0 |= TIME_TRUNCATE_FRACTIONAL;
    }
    m.render()
}

/// Whether two `sql_mode` strings name the same modes (any order, any
/// case).
#[must_use]
pub fn same_modes(a: &str, b: &str) -> bool {
    let set = |s: &str| {
        let mut v: Vec<String> = s
            .split(',')
            .map(|i| i.trim().to_ascii_uppercase())
            .filter(|i| !i.is_empty())
            .collect();
        v.sort();
        v.dedup();
        v
    };
    set(a) == set(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_programs_record_and_show_the_client_mode() {
        let d = MYSQL84_DEFAULT;
        let rec = d.stored_program();
        assert_eq!(
            rec,
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,\
             NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL,NO_AUTO_CREATE_USER"
        );
        assert_eq!(stored_program_view(&rec), d.render());
        let p = parse_mysql("PIPES_AS_CONCAT").unwrap();
        assert_eq!(p.stored_program(), "PIPES_AS_CONCAT,TIME_ROUND_FRACTIONAL");
        assert_eq!(stored_program_view(&p.stored_program()), "PIPES_AS_CONCAT");
        // MariaDB's default, as stored programs had before (sprig_ctl's).
        assert_eq!(
            stored_program_view(
                "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
            ),
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,\
             NO_ENGINE_SUBSTITUTION,TIME_TRUNCATE_FRACTIONAL"
        );
        assert_eq!(Mode(0).stored_program(), "TIME_ROUND_FRACTIONAL");
        assert_eq!(stored_program_view("TIME_ROUND_FRACTIONAL"), "");
    }

    // Captured from MySQL 8.4.11 and MariaDB 11.8.9 on 9 November 2026.
    #[test]
    fn mysql_rendering_and_combinations() {
        let all = "TIME_TRUNCATE_FRACTIONAL,PAD_CHAR_TO_FULL_LENGTH,STRICT_ALL_TABLES,\
                   NO_ENGINE_SUBSTITUTION,HIGH_NOT_PRECEDENCE,NO_ZERO_DATE,NO_ZERO_IN_DATE,\
                   ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,\
                   NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,NO_DIR_IN_CREATE,\
                   NO_UNSIGNED_SUBTRACTION,ONLY_FULL_GROUP_BY,IGNORE_SPACE,ANSI_QUOTES,\
                   PIPES_AS_CONCAT,REAL_AS_FLOAT";
        assert_eq!(
            parse_mysql(all).unwrap().render(),
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,\
             NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,NO_AUTO_VALUE_ON_ZERO,\
             NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,\
             NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,\
             HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH,\
             TIME_TRUNCATE_FRACTIONAL"
        );
        assert_eq!(
            parse_mysql("ANSI").unwrap().render(),
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
        );
        assert_eq!(
            parse_mysql("traditional").unwrap().render(),
            "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION"
        );
        assert_eq!(
            parse_mysql("STRICT_TRANS_TABLES,,ANSI").unwrap().render(),
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI,\
             STRICT_TRANS_TABLES"
        );
        assert_eq!(parse_mysql("").unwrap().render(), "");
        assert_eq!(
            MYSQL84_DEFAULT.render(),
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
        );
        assert_eq!(from_number(1).unwrap().render(), "REAL_AS_FLOAT");
    }

    #[test]
    fn mysql_refuses_what_it_removed_or_never_had() {
        for bad in ["MAXDB", "ORACLE", "BOGUS"] {
            assert_eq!(parse_mysql(bad), Err(BadValue(bad.to_owned())));
        }
        assert_eq!(
            parse_mysql(" strict_trans_tables ").unwrap_err().message(),
            "Variable 'sql_mode' can't be set to the value of ' strict_trans_tables '"
        );
        assert!(from_number(1 << 40).is_err());
    }

    #[test]
    fn backend_values() {
        assert_eq!(
            MYSQL84_DEFAULT.backend(),
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,\
             NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL"
        );
        assert_eq!(
            parse_mysql("ANSI").unwrap().backend(),
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,TIME_ROUND_FRACTIONAL"
        );
        assert_eq!(
            parse_mysql("TIME_TRUNCATE_FRACTIONAL").unwrap().backend(),
            ""
        );
        assert_eq!(parse_mysql("").unwrap().backend(), "TIME_ROUND_FRACTIONAL");
    }

    #[test]
    fn mariadb_values_read_back() {
        let m = from_mariadb(
            "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_AUTO_CREATE_USER,\
             NO_ENGINE_SUBSTITUTION,TIME_ROUND_FRACTIONAL",
        );
        assert_eq!(m, parse_mysql("TRADITIONAL").unwrap());
        // A dump's routine mode restores.
        assert_eq!(
            parse_mysql("STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER").unwrap(),
            parse_mysql("STRICT_TRANS_TABLES").unwrap()
        );
        // The client's view restored from a variable keeps ONLY_FULL_GROUP_BY.
        let v = from_mariadb(&MYSQL84_DEFAULT.render());
        assert_eq!(v, MYSQL84_DEFAULT);
        assert!(same_modes("a,B", "b, A"));
    }
}
