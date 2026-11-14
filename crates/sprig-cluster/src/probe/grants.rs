//! `SHOW GRANTS` parsing and the privilege facts the profile needs (the
//! system accounts' grants).
//!
//! A missing privilege disables only what needs it, so every privilege is
//! its own flag. Role grants are recorded but not expanded: the system
//! accounts are expected to hold their privileges directly.

use serde::{Deserialize, Serialize};
use sprig_common::names::{SYS_SCHEMA, TENANT_SCHEMAS_GRANT_PATTERN};

/// What a grant applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantTarget {
    /// `*.*`.
    Global,
    /// `` `db`.* ``; the name is a `LIKE` pattern.
    Database(String),
    /// `` `db`.`table` ``.
    Table(String, String),
    /// `PROCEDURE`, `FUNCTION`, `PACKAGE` or `PACKAGE BODY` grants.
    Routine(String, String),
}

/// One parsed `GRANT` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grant {
    /// `GRANT <privileges> ON <target> TO ...`.
    Privileges {
        /// Upper-case privilege names, aliases normalized; column lists
        /// dropped.
        privileges: Vec<String>,
        /// The target.
        target: GrantTarget,
        /// `WITH GRANT OPTION`.
        grant_option: bool,
    },
    /// `GRANT <role> TO ...`.
    Role(String),
}

impl Grant {
    /// Parses one line; `None` for `PROXY` grants and anything unrecognized.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let toks = tokenize(line)?;
        let mut it = toks.iter().peekable();
        if !matches!(it.next(), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("GRANT")) {
            return None;
        }
        // Privileges, up to the top-level ON (or TO for a role grant).
        let mut privileges = Vec::new();
        let mut current: Vec<String> = Vec::new();
        let mut depth = 0usize;
        let mut role = None;
        loop {
            match it.next()? {
                Tok::Open => depth += 1,
                Tok::Close => depth = depth.checked_sub(1)?,
                _ if depth > 0 => {}
                Tok::Word(w) if w.eq_ignore_ascii_case("ON") => break,
                Tok::Word(w) if w.eq_ignore_ascii_case("TO") => {
                    return role.map(Grant::Role);
                }
                Tok::Comma => privileges.push(std::mem::take(&mut current).join(" ")),
                Tok::Word(w) => current.push(w.to_ascii_uppercase()),
                Tok::Quoted(q) => role = Some(q.clone()),
                _ => {}
            }
        }
        if !current.is_empty() {
            privileges.push(current.join(" "));
        }
        if privileges.iter().any(|p| p == "PROXY") {
            return None;
        }
        let privileges = privileges.into_iter().map(normalize).collect();

        // Target: [PROCEDURE|FUNCTION|PACKAGE [BODY]] db.obj
        let mut routine = false;
        while let Some(Tok::Word(w)) = it.peek() {
            if ["PROCEDURE", "FUNCTION", "PACKAGE", "BODY"]
                .iter()
                .any(|k| w.eq_ignore_ascii_case(k))
            {
                routine = true;
                it.next();
            } else {
                break;
            }
        }
        let db = name(it.next()?)?;
        if !matches!(it.next()?, Tok::Dot) {
            return None;
        }
        let obj = name(it.next()?)?;
        let target = match (db.as_str(), obj.as_str(), routine) {
            ("*", "*", _) => GrantTarget::Global,
            (_, "*", false) => GrantTarget::Database(db),
            (_, _, false) => GrantTarget::Table(db, obj),
            (_, _, true) => GrantTarget::Routine(db, obj),
        };
        // WITH GRANT OPTION anywhere after TO.
        let words: Vec<&str> = it
            .filter_map(|t| match t {
                Tok::Word(w) => Some(w.as_str()),
                _ => None,
            })
            .collect();
        let grant_option = words.windows(3).any(|w| {
            w[0].eq_ignore_ascii_case("WITH")
                && w[1].eq_ignore_ascii_case("GRANT")
                && w[2].eq_ignore_ascii_case("OPTION")
        });
        Some(Grant::Privileges {
            privileges,
            target,
            grant_option,
        })
    }
}

/// Aliases MariaDB accepts, mapped to the name `SHOW GRANTS` prints.
fn normalize(p: String) -> String {
    match p.as_str() {
        "REPLICA MONITOR" => "SLAVE MONITOR".to_owned(),
        "REPLICATION CLIENT" => "BINLOG MONITOR".to_owned(),
        "REPLICATION REPLICA" => "REPLICATION SLAVE".to_owned(),
        "ALL" => "ALL PRIVILEGES".to_owned(),
        _ => p,
    }
}

fn name(t: &Tok) -> Option<String> {
    match t {
        Tok::Quoted(q) | Tok::Word(q) => Some(q.clone()),
        Tok::Star => Some("*".to_owned()),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Quoted(String),
    Str,
    Comma,
    Dot,
    Star,
    At,
    Open,
    Close,
}

/// Splits a grant line into tokens; `None` on an unterminated quote.
fn tokenize(s: &str) -> Option<Vec<Tok>> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => {}
            ',' => out.push(Tok::Comma),
            '.' => out.push(Tok::Dot),
            '*' => out.push(Tok::Star),
            '@' => out.push(Tok::At),
            '(' => out.push(Tok::Open),
            ')' => out.push(Tok::Close),
            '`' => {
                let mut q = String::new();
                loop {
                    match chars.next()? {
                        '`' if chars.peek() == Some(&'`') => {
                            chars.next();
                            q.push('`');
                        }
                        '`' => break,
                        c => q.push(c),
                    }
                }
                out.push(Tok::Quoted(q));
            }
            '\'' => {
                loop {
                    match chars.next()? {
                        '\\' => {
                            chars.next()?;
                        }
                        '\'' if chars.peek() == Some(&'\'') => {
                            chars.next();
                        }
                        '\'' => break,
                        _ => {}
                    }
                }
                out.push(Tok::Str);
            }
            c => {
                let mut w = String::from(c);
                while let Some(&n) = chars.peek() {
                    if n.is_alphanumeric() || n == '_' || n == '%' || n == '$' {
                        w.push(n);
                        chars.next();
                    } else {
                        break;
                    }
                }
                out.push(Tok::Word(w));
            }
        }
    }
    Some(out)
}

/// Privilege facts about the probing account (`sprig_ctl` in production).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // one flag per privilege, by design
pub struct PrivilegeInfo {
    /// `ALL` on every tenant schema (`` `t%` ``) or globally.
    pub tenant_schemas_all: bool,
    /// ... `WITH GRANT OPTION`.
    pub tenant_schemas_grant_option: bool,
    /// `ALL` on `__sprig_sys` or globally.
    pub sys_schema_all: bool,
    /// `CREATE USER`.
    pub create_user: bool,
    /// `PROCESS`.
    pub process: bool,
    /// `CONNECTION ADMIN`.
    pub connection_admin: bool,
    /// `SLAVE MONITOR`.
    pub slave_monitor: bool,
    /// `BINLOG MONITOR`.
    pub binlog_monitor: bool,
    /// `BINLOG ADMIN`.
    pub binlog_admin: bool,
    /// `READ_ONLY ADMIN`.
    pub read_only_admin: bool,
    /// `SET USER`: explicit `DEFINER`.
    pub set_user: bool,
    /// `REPLICATION SLAVE`.
    pub replication_slave: bool,
    /// `SELECT` on `mysql.db`.
    pub read_mysql_db: bool,
    /// `SUPER` (never needed; reported).
    pub super_priv: bool,
    /// Active test: `SET SESSION sql_log_bin` worked (deep probe).
    pub sql_log_bin_settable: Option<bool>,
    /// Active test on Galera: `SET SESSION wsrep_OSU_method` worked.
    pub osu_method_settable: Option<bool>,
}

impl PrivilegeInfo {
    /// Folds parsed grants into flags.
    #[must_use]
    pub fn from_grants(grants: &[Grant]) -> Self {
        let mut p = Self::default();
        let tenant_probe = "t1_d1_g1_s1";
        for g in grants {
            let Grant::Privileges {
                privileges,
                target,
                grant_option,
            } = g
            else {
                continue;
            };
            let has = |name: &str| {
                privileges
                    .iter()
                    .any(|x| x == name || x == "ALL PRIVILEGES")
            };
            let all = privileges.iter().any(|x| x == "ALL PRIVILEGES");
            match target {
                GrantTarget::Global => {
                    p.tenant_schemas_all |= all;
                    p.tenant_schemas_grant_option |= all && *grant_option;
                    p.sys_schema_all |= all;
                    p.create_user |= has("CREATE USER");
                    p.process |= has("PROCESS");
                    p.connection_admin |= has("CONNECTION ADMIN");
                    p.slave_monitor |= has("SLAVE MONITOR");
                    p.binlog_monitor |= has("BINLOG MONITOR");
                    p.binlog_admin |= has("BINLOG ADMIN");
                    p.read_only_admin |= has("READ_ONLY ADMIN");
                    p.set_user |= has("SET USER");
                    p.replication_slave |= has("REPLICATION SLAVE");
                    p.read_mysql_db |= has("SELECT");
                    p.super_priv |= has("SUPER");
                }
                GrantTarget::Database(pattern) => {
                    if all && like(pattern, tenant_probe) {
                        p.tenant_schemas_all = true;
                        p.tenant_schemas_grant_option |= *grant_option;
                    }
                    p.sys_schema_all |= all && like(pattern, SYS_SCHEMA);
                    p.read_mysql_db |= has("SELECT") && like(pattern, "mysql");
                }
                GrantTarget::Table(db, table) => {
                    p.read_mysql_db |= has("SELECT") && db == "mysql" && table == "db";
                }
                GrantTarget::Routine(..) => {}
            }
        }
        p
    }

    /// The `GRANT` statements that would give `sprig_ctl` what it lacks,
    /// for registration reports.
    #[must_use]
    pub fn missing_ctl_grants(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !(self.tenant_schemas_all && self.tenant_schemas_grant_option) {
            out.push(format!(
                "GRANT ALL ON `{TENANT_SCHEMAS_GRANT_PATTERN}`.* TO sprig_ctl WITH GRANT OPTION"
            ));
        }
        if !self.sys_schema_all {
            out.push(
                "GRANT ALL ON `\\_\\_sprig\\_sys`.* TO sprig_ctl WITH GRANT OPTION".to_owned(),
            );
        }
        let globals = [
            (self.create_user, "CREATE USER"),
            (self.process, "PROCESS"),
            (self.connection_admin, "CONNECTION ADMIN"),
            (self.slave_monitor, "SLAVE MONITOR"),
            (self.binlog_monitor, "BINLOG MONITOR"),
            (self.binlog_admin, "BINLOG ADMIN"),
            (self.read_only_admin, "READ_ONLY ADMIN"),
            (self.set_user, "SET USER"),
        ];
        let missing: Vec<&str> = globals
            .iter()
            .filter(|(h, _)| !h)
            .map(|(_, n)| *n)
            .collect();
        if !missing.is_empty() {
            out.push(format!("GRANT {} ON *.* TO sprig_ctl", missing.join(", ")));
        }
        if !self.read_mysql_db {
            out.push("GRANT SELECT ON mysql.db TO sprig_ctl".to_owned());
        }
        out
    }
}

/// SQL `LIKE` with `%`, `_` and `\` escapes, as MariaDB matches database
/// grant patterns.
fn like(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('%', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some(('_', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some(('\\', rest)) if !rest.is_empty() => {
                t.first() == Some(&rest[0]) && go(&rest[1..], &t[1..])
            }
            Some((c, rest)) => t.first() == Some(c) && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn priv_info(lines: &[&str]) -> PrivilegeInfo {
        PrivilegeInfo::from_grants(
            &lines
                .iter()
                .filter_map(|l| Grant::parse(l))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn root_has_everything() {
        let p = priv_info(&[
            "GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION",
            "GRANT PROXY ON ``@`%` TO `root`@`localhost` WITH GRANT OPTION",
        ]);
        assert!(p.tenant_schemas_all && p.tenant_schemas_grant_option && p.sys_schema_all);
        assert!(p.create_user && p.connection_admin && p.set_user && p.read_mysql_db);
        assert!(p.missing_ctl_grants().is_empty());
    }

    #[test]
    fn sprig_ctl_as_chapter_09_grants_it() {
        let p = priv_info(&[
            "GRANT CREATE USER, PROCESS, CONNECTION ADMIN, SLAVE MONITOR, BINLOG MONITOR, BINLOG ADMIN, READ_ONLY ADMIN, SET USER ON *.* TO `sprig_ctl`@`%` REQUIRE SSL",
            "GRANT ALL PRIVILEGES ON `t%`.* TO `sprig_ctl`@`%` WITH GRANT OPTION",
            "GRANT ALL PRIVILEGES ON `\\_\\_sprig\\_sys`.* TO `sprig_ctl`@`%` WITH GRANT OPTION",
            "GRANT SELECT ON `mysql`.`db` TO `sprig_ctl`@`%`",
        ]);
        assert!(p.tenant_schemas_all && p.tenant_schemas_grant_option);
        assert!(p.sys_schema_all && p.read_mysql_db && p.set_user && p.binlog_admin);
        assert!(!p.super_priv && !p.replication_slave);
        assert!(
            p.missing_ctl_grants().is_empty(),
            "{:?}",
            p.missing_ctl_grants()
        );
    }

    #[test]
    fn missing_privileges_are_reported_with_their_fix() {
        let p = priv_info(&[
            "GRANT PROCESS, REPLICA MONITOR ON *.* TO `sprig_ctl`@`%`",
            "GRANT ALL PRIVILEGES ON `t%`.* TO `sprig_ctl`@`%`",
        ]);
        assert!(p.slave_monitor && p.tenant_schemas_all && !p.tenant_schemas_grant_option);
        assert!(!p.connection_admin);
        let fixes = p.missing_ctl_grants().join("\n");
        assert!(fixes.contains("WITH GRANT OPTION"));
        assert!(fixes.contains("CONNECTION ADMIN"));
        assert!(!fixes.contains("PROCESS"));
    }

    #[test]
    fn parses_targets_columns_roles_and_aliases() {
        assert_eq!(
            Grant::parse("GRANT SELECT (`a`, `b`), INSERT ON `d`.`t` TO `u`@`%`"),
            Some(Grant::Privileges {
                privileges: vec!["SELECT".into(), "INSERT".into()],
                target: GrantTarget::Table("d".into(), "t".into()),
                grant_option: false,
            })
        );
        assert_eq!(
            Grant::parse("GRANT EXECUTE ON PACKAGE BODY `__sprig_sys`.`p` TO `u`@`%`"),
            Some(Grant::Privileges {
                privileges: vec!["EXECUTE".into()],
                target: GrantTarget::Routine("__sprig_sys".into(), "p".into()),
                grant_option: false,
            })
        );
        assert_eq!(
            Grant::parse("GRANT `ops` TO `sprig_ctl`@`%`"),
            Some(Grant::Role("ops".into()))
        );
        assert_eq!(
            Grant::parse("GRANT REPLICATION CLIENT ON *.* TO `r`@`%`"),
            Some(Grant::Privileges {
                privileges: vec!["BINLOG MONITOR".into()],
                target: GrantTarget::Global,
                grant_option: false,
            })
        );
        assert_eq!(
            Grant::parse("GRANT PROXY ON ``@`%` TO `root`@`localhost`"),
            None
        );
        assert_eq!(Grant::parse("REVOKE ALL ON *.* FROM x"), None);
        assert_eq!(Grant::parse("GRANT SELECT ON `unterminated"), None);
        assert_eq!(Grant::parse(""), None);
    }

    #[test]
    fn like_patterns() {
        assert!(like("t%", "t42_d1_g1_s1"));
        assert!(!like("t%", "__sprig_sys"));
        assert!(like("\\_\\_sprig\\_sys", "__sprig_sys"));
        assert!(!like("\\_\\_sprig\\_sys", "xxsprigxsys"));
        assert!(like("t%\\_sprig", "t42_sprig"));
        assert!(!like("t%\\_sprig", "t42_d1_g1_s1"));
        assert!(like("_", "a") && !like("_", ""));
    }
}
