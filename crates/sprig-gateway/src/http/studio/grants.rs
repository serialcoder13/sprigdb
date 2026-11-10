//! What a user's `SHOW GRANTS` lines allow, as far as Studio's routes ask
//! (chapter 29's privileges, read the way a client would read them).
//!
//! Studio hides what a user can't do as a courtesy; the gateway decides
//! every statement. The one place the gateway itself uses this is the
//! indexing actions (approve, reject, freeze, drop), which go to the
//! controller rather than through a statement: chapter 44 lets only users
//! who could run the `CREATE INDEX` approve, so the caller needs `INDEX`,
//! `ALTER` or `ALL` on the database (or `*.*`), or `SPRIG_TENANT_ADMIN`.

use serde_json::{Value, json};

/// The privileges Studio's views and routes look at.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // one flag per privilege, as WhoAmI shows them
pub struct Privileges {
    /// `SPRIG_TENANT_ADMIN` (or `ALL` on `*.*`).
    pub tenant_admin: bool,
    /// `SPRIG_SETTINGS_ADMIN` (or tenant admin).
    pub settings_admin: bool,
    /// `CREATE USER` (or tenant admin).
    pub create_user: bool,
    /// `GRANT OPTION` on something.
    pub grant_option: bool,
    /// `INDEX`, `ALTER` or `ALL` on `*.*`.
    pub index_all: bool,
    /// Databases with `INDEX`, `ALTER` or `ALL` on `` `db`.* ``.
    pub index_dbs: Vec<String>,
}

/// Removes the backquotes around a name and undoes doubled ones.
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('`') && t.ends_with('`') {
        t[1..t.len() - 1].replace("``", "`")
    } else {
        t.to_owned()
    }
}

/// The `ON` target of a grant: `(db, table)`, `*` for wildcards.
fn target(on: &str) -> (String, String) {
    // `db`.`t`, `db`.*, *.* (a backquoted name may hold a dot).
    let mut in_q = false;
    let mut split = None;
    for (i, c) in on.char_indices() {
        match c {
            '`' => in_q = !in_q,
            '.' if !in_q => {
                split = Some(i);
                break;
            }
            _ => {}
        }
    }
    match split {
        Some(i) => (unquote(&on[..i]), unquote(&on[i + 1..])),
        None => (unquote(on), "*".to_owned()),
    }
}

impl Privileges {
    /// Reads `SHOW GRANTS` lines.
    #[must_use]
    pub fn of(lines: &[String]) -> Self {
        let mut p = Self::default();
        for line in lines {
            let u = line.trim();
            let Some(rest) = u
                .strip_prefix("GRANT ")
                .or_else(|| u.strip_prefix("grant "))
            else {
                continue;
            };
            let upper = rest.to_ascii_uppercase();
            // Role grants (`GRANT r TO u`) have no ON.
            let Some(on_at) = upper.find(" ON ") else {
                continue;
            };
            let privs = &upper[..on_at];
            let after = &rest[on_at + 4..];
            let to_at = after
                .to_ascii_uppercase()
                .find(" TO ")
                .unwrap_or(after.len());
            let (db, table) = target(after[..to_at].trim());
            if upper.contains("WITH GRANT OPTION") {
                p.grant_option = true;
            }
            let list: Vec<&str> = privs.split(',').map(str::trim).collect();
            let all = list.iter().any(|x| *x == "ALL" || *x == "ALL PRIVILEGES");
            let has = |name: &str| all || list.contains(&name);
            if db == "*" {
                if has("SPRIG_TENANT_ADMIN") || all {
                    p.tenant_admin = true;
                }
                if has("SPRIG_SETTINGS_ADMIN") {
                    p.settings_admin = true;
                }
                if has("CREATE USER") {
                    p.create_user = true;
                }
                if has("INDEX") || has("ALTER") {
                    p.index_all = true;
                }
            } else if table == "*" && (has("INDEX") || has("ALTER")) && !p.index_dbs.contains(&db) {
                p.index_dbs.push(db);
            }
        }
        if p.tenant_admin {
            p.settings_admin = true;
            p.create_user = true;
            p.index_all = true;
        }
        p
    }

    /// Whether the user could create an index in `db`.
    #[must_use]
    pub fn may_index(&self, db: &str) -> bool {
        self.index_all || self.index_dbs.iter().any(|d| d.eq_ignore_ascii_case(db))
    }

    /// `WhoAmI`'s `privileges`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "tenant_admin": self.tenant_admin,
            "settings_admin": self.settings_admin,
            "create_user": self.create_user,
            "grant_option": self.grant_option,
            "index_all": self.index_all,
            "index_dbs": self.index_dbs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(lines: &[&str]) -> Privileges {
        Privileges::of(&lines.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
    }

    #[test]
    fn reads_the_usual_lines() {
        let p = of(&[
            "GRANT USAGE ON *.* TO `app`@`%`",
            "GRANT SELECT, INSERT, INDEX ON `shop`.* TO `app`@`%`",
            "GRANT SELECT ON `crm`.`leads` TO `app`@`%`",
        ]);
        assert!(!p.tenant_admin);
        assert!(p.may_index("shop"));
        assert!(p.may_index("SHOP"));
        assert!(!p.may_index("crm"));
        assert!(!p.create_user);
    }

    #[test]
    fn all_and_admin() {
        let p = of(&["GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"]);
        assert!(p.tenant_admin && p.settings_admin && p.create_user && p.grant_option);
        assert!(p.may_index("anything"));
        let p = of(&["GRANT SPRIG_TENANT_ADMIN ON *.* TO `a`@`%`"]);
        assert!(p.tenant_admin && p.may_index("x"));
        let p = of(&["GRANT SPRIG_SETTINGS_ADMIN, CREATE USER ON *.* TO `a`@`%`"]);
        assert!(!p.tenant_admin && p.settings_admin && p.create_user);
        assert!(!p.may_index("x"));
    }

    #[test]
    fn odd_names_and_role_grants() {
        let p = of(&[
            "GRANT `dev`@`%` TO `a`@`%`",
            "GRANT ALTER ON `we.ird``db`.* TO `a`@`%`",
        ]);
        assert_eq!(p.index_dbs, ["we.ird`db"]);
        assert_eq!(target("`a`.`b.c`"), ("a".to_owned(), "b.c".to_owned()));
    }
}
