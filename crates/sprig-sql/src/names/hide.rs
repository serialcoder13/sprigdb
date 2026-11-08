//! Hiding internal objects (chapter 07, "Hiding and reserved objects";
//! build step 8): what a session may see, the `SHOW CREATE TABLE` line
//! filter, `CREATE TABLE ... LIKE`, and the original text of routines.
//!
//! | Object | Hidden from | Shown by |
//! |---|---|---|
//! | `__sprig_` tables, views, routines, triggers, events | every `SHOW` and `information_schema` view | `@sprig_show_internal` |
//! | `_sprig_` columns, and indexes and constraints on them | `SHOW COLUMNS`, `SHOW INDEX`, `SHOW CREATE TABLE`, the views | `@sprig_show_internal` |
//! | `sprig_auto_` indexes | `SHOW INDEX`, `SHOW CREATE TABLE`, `STATISTICS` | `@sprig_show_auto_indexes` (or `@sprig_show_internal`) |
//! | the invisible primary key `my_row_id` | the same | `show_gipk_in_create_table_and_information_schema` (`ON` on 3306, `OFF` on 3307) |

use sprig_common::names::{GIPK_COLUMN, is_auto_index, is_internal_column, is_internal_object};
use sprig_proto_mysql::packets::Persona;

/// What the session may see of SprigDB's internal objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visibility {
    /// `@sprig_show_auto_indexes = 1`.
    pub auto_indexes: bool,
    /// `@sprig_show_internal = 1`, for a user with the tenant's `ALL`
    /// privilege (the gateway checks it): every internal object, read-only.
    pub internal: bool,
    /// `show_gipk_in_create_table_and_information_schema`: show
    /// `my_row_id`.
    pub gipk: bool,
}

impl Visibility {
    /// The defaults for a persona: nothing internal, and the GIPK shown on
    /// the MySQL persona only, as MySQL 8.4 does.
    #[must_use]
    pub const fn for_persona(persona: Persona) -> Self {
        Self {
            auto_indexes: false,
            internal: false,
            gipk: matches!(persona, Persona::MySql84),
        }
    }

    /// Nothing internal at all: what `CREATE TABLE ... LIKE` copies.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            auto_indexes: false,
            internal: false,
            gipk: false,
        }
    }

    /// Whether a table, view, routine, trigger or event is hidden.
    #[must_use]
    pub fn hides_object(&self, name: &str) -> bool {
        !self.internal && is_internal_object(name)
    }

    /// Whether a column is hidden.
    #[must_use]
    pub fn hides_column(&self, name: &str) -> bool {
        (!self.internal && is_internal_column(name))
            || (!self.gipk && !self.internal && name.eq_ignore_ascii_case(GIPK_COLUMN))
    }

    /// Whether an index named `name` over `columns` is hidden: an
    /// automatic index, an index on a hidden column, or the GIPK's primary
    /// key.
    #[must_use]
    pub fn hides_index<'c>(&self, name: &str, columns: impl IntoIterator<Item = &'c str>) -> bool {
        if is_auto_index(name) && !self.auto_indexes && !self.internal {
            return true;
        }
        columns.into_iter().any(|c| self.hides_column(c))
    }
}

/// One line of MariaDB's `SHOW CREATE TABLE` body.
#[derive(Debug, PartialEq, Eq)]
enum Line<'a> {
    /// `` `name` type ... ``.
    Column(String),
    /// `PRIMARY KEY`, `UNIQUE KEY`, `KEY`, `FULLTEXT KEY`, `SPATIAL KEY`,
    /// `VECTOR KEY`: the index name (`PRIMARY` for the primary key) and its
    /// columns.
    Index(String, Vec<String>),
    /// `CONSTRAINT ... FOREIGN KEY (cols) REFERENCES t (cols)`: the
    /// constraint's columns, and the referenced table's name.
    ForeignKey(Vec<String>, String),
    /// `CONSTRAINT ... CHECK (expr)`: the backquoted names in it.
    Check(Vec<String>),
    /// Anything else (`PERIOD FOR`, a check without a name).
    Other(&'a str),
}

/// The backquoted identifiers in `s`, unescaped.
fn backquoted(s: &str) -> Vec<String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'`' {
            i += 1;
            continue;
        }
        let mut name = Vec::new();
        i += 1;
        while i < b.len() {
            if b[i] == b'`' {
                if b.get(i + 1) == Some(&b'`') {
                    name.push(b'`');
                    i += 2;
                    continue;
                }
                break;
            }
            name.push(b[i]);
            i += 1;
        }
        i += 1;
        out.push(String::from_utf8_lossy(&name).into_owned());
    }
    out
}

/// The text from the first `(` at or after `from` to its matching `)`,
/// skipping backquoted names and strings.
fn paren_group(s: &str, from: usize) -> Option<&str> {
    let b = s.as_bytes();
    let open = from + s.get(from..)?.find('(')?;
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'`' | b'\'' | b'"' => {
                let q = b[i];
                i += 1;
                while i < b.len() && b[i] != q {
                    if b[i] == b'\\' && q != b'`' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return s.get(open + 1..i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn classify(line: &str) -> Line<'_> {
    let t = line.trim_start();
    if t.starts_with('`') {
        return backquoted(t)
            .into_iter()
            .next()
            .map_or(Line::Other(line), Line::Column);
    }
    let upper = t.get(..t.len().min(24)).unwrap_or(t).to_ascii_uppercase();
    if upper.starts_with("PRIMARY KEY") {
        let cols = paren_group(t, 0).map(backquoted).unwrap_or_default();
        return Line::Index("PRIMARY".to_owned(), cols);
    }
    for kw in [
        "UNIQUE KEY",
        "KEY",
        "FULLTEXT KEY",
        "SPATIAL KEY",
        "VECTOR KEY",
        "UNIQUE INDEX",
        "INDEX",
    ] {
        if upper.starts_with(kw) && t.as_bytes().get(kw.len()) == Some(&b' ') {
            let rest = &t[kw.len()..];
            let name = backquoted(rest.split('(').next().unwrap_or_default())
                .into_iter()
                .next()
                .unwrap_or_default();
            let at = rest.find('(').unwrap_or(0);
            let cols = paren_group(rest, at).map(backquoted).unwrap_or_default();
            return Line::Index(name, cols);
        }
    }
    if upper.starts_with("CONSTRAINT") {
        let up = t.to_ascii_uppercase();
        if let Some(fk) = up.find(" FOREIGN KEY ") {
            let cols = paren_group(t, fk).map(backquoted).unwrap_or_default();
            let referenced = up
                .find(" REFERENCES ")
                .map(|r| {
                    let rest = &t[r + " REFERENCES ".len()..];
                    let head = rest.split('(').next().unwrap_or_default();
                    backquoted(head).pop().unwrap_or_default()
                })
                .unwrap_or_default();
            return Line::ForeignKey(cols, referenced);
        }
        if let Some(ck) = up.find(" CHECK ") {
            return Line::Check(backquoted(&t[ck..]));
        }
    }
    Line::Other(line)
}

/// The `SHOW CREATE TABLE` line filter (chapter 07). MariaDB prints one
/// column, key or constraint per line between `CREATE TABLE ... (` and
/// `) ENGINE=...`; this drops the lines `vis` hides (columns, indexes and
/// foreign keys on hidden columns, checks naming them, automatic indexes,
/// the GIPK, foreign keys to internal tables) and fixes the trailing
/// comma. Text that doesn't have that shape is returned unchanged. Schema
/// names are left for the caller ([`super::output`]).
#[must_use]
pub fn filter_create_table(text: &str, vis: &Visibility) -> String {
    filter_lines(text, |line| hidden_line(line, *vis))
}

fn hidden_line(line: &Line<'_>, vis: Visibility) -> bool {
    match line {
        Line::Column(name) => vis.hides_column(name),
        Line::Index(name, cols) => vis.hides_index(name, cols.iter().map(String::as_str)),
        Line::ForeignKey(cols, referenced) => {
            cols.iter().any(|c| vis.hides_column(c)) || vis.hides_object(referenced)
        }
        Line::Check(names) => names.iter().any(|c| vis.hides_column(c)),
        Line::Other(_) => false,
    }
}

fn filter_lines(text: &str, mut drop: impl FnMut(&Line<'_>) -> bool) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let Some(close) = lines.iter().rposition(|l| l.starts_with(')')) else {
        return text.to_owned();
    };
    if close == 0 || !lines[0].trim_end().ends_with('(') {
        return text.to_owned();
    }
    let mut body: Vec<String> = Vec::new();
    for l in &lines[1..close] {
        let item = l.trim_end().strip_suffix(',').unwrap_or(l.trim_end());
        if !drop(&classify(item)) {
            body.push(item.to_owned());
        }
    }
    let mut out = String::with_capacity(text.len());
    out.push_str(lines[0]);
    for (i, b) in body.iter().enumerate() {
        out.push('\n');
        out.push_str(b);
        if i + 1 < body.len() {
            out.push(',');
        }
    }
    for l in &lines[close..] {
        out.push('\n');
        out.push_str(l);
    }
    out
}

/// The statement `CREATE TABLE new LIKE old` runs as (chapter 07):
/// `head` is the client's statement up to the `LIKE` (or the `(` before
/// it), already rewritten (``CREATE TABLE IF NOT EXISTS `t42_d1_g1_s1`.new``),
/// and `create_text` the `Create Table` value of `SHOW CREATE TABLE old`.
///
/// The copy leaves out everything internal (whatever the session may
/// see), and, as MySQL's `LIKE` does, foreign keys and the
/// `AUTO_INCREMENT` counter.
///
/// # Errors
///
/// `None` when `create_text` doesn't start with `CREATE TABLE ... (`,
/// such as for a view.
#[must_use]
pub fn create_like_sql(head: &str, create_text: &str) -> Option<String> {
    let vis = Visibility::none();
    let filtered = filter_lines(create_text, |line| {
        matches!(line, Line::ForeignKey(..)) || hidden_line(line, vis)
    });
    let first_nl = filtered.find('\n')?;
    let first = &filtered[..first_nl];
    let upper = first.to_ascii_uppercase();
    if !upper.starts_with("CREATE TABLE ") && !upper.starts_with("CREATE TEMPORARY TABLE ")
        || !first.ends_with('(')
    {
        return None;
    }
    let rest = &filtered[first_nl..];
    let rest = strip_table_option(rest, "AUTO_INCREMENT=");
    Some(format!("{} ({rest}", head.trim_end()))
}

/// Removes ` KEY=value` from the closing line of a `CREATE TABLE` text.
fn strip_table_option(rest: &str, key: &str) -> String {
    let Some(close) = rest.rfind("\n)") else {
        return rest.to_owned();
    };
    let (body, tail) = rest.split_at(close);
    let Some(at) = tail.find(&format!(" {key}")) else {
        return rest.to_owned();
    };
    let after = &tail[at + 1 + key.len()..];
    let end = after.find(' ').unwrap_or(after.len());
    format!("{body}{}{}", &tail[..at], &after[end..])
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDERS: &str = "CREATE TABLE `orders` (
  `id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  `_sprig_p_status` varchar(20) DEFAULT NULL INVISIBLE,
  PRIMARY KEY (`id`),
  KEY `sprig_auto_3fa91c0e` (`customer_id`),
  KEY `idx_mixed` (`account_id`,`_sprig_p_status`),
  CONSTRAINT `fk_acct` FOREIGN KEY (`account_id`) REFERENCES `t9999_d2_g2_s2`.`accounts` (`id`),
  CONSTRAINT `fk_lk` FOREIGN KEY (`id`) REFERENCES `__sprig_lk_x` (`id`),
  CONSTRAINT `chk_status` CHECK (`_sprig_p_status` <> '')
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4";

    #[test]
    fn line_filter_drops_hidden_lines_and_fixes_the_comma() {
        let out = filter_create_table(ORDERS, &Visibility::for_persona(Persona::MariaDb));
        assert_eq!(
            out,
            "CREATE TABLE `orders` (
  `id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_acct` FOREIGN KEY (`account_id`) REFERENCES `t9999_d2_g2_s2`.`accounts` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4"
        );
        let shown = Visibility {
            auto_indexes: true,
            ..Visibility::for_persona(Persona::MariaDb)
        };
        assert!(filter_create_table(ORDERS, &shown).contains("sprig_auto_3fa91c0e"));
        let all = Visibility {
            internal: true,
            ..Visibility::none()
        };
        assert_eq!(filter_create_table(ORDERS, &all), ORDERS);
    }

    #[test]
    fn gipk_follows_the_variable() {
        let t = "CREATE TABLE `g` (\n  `my_row_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT INVISIBLE,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`my_row_id`)\n) ENGINE=InnoDB";
        assert_eq!(
            filter_create_table(t, &Visibility::for_persona(Persona::MariaDb)),
            "CREATE TABLE `g` (\n  `v` int(11) DEFAULT NULL\n) ENGINE=InnoDB"
        );
        assert_eq!(
            filter_create_table(t, &Visibility::for_persona(Persona::MySql84)),
            t
        );
    }

    #[test]
    fn odd_shapes_pass_through() {
        for t in ["", "CREATE VIEW v AS select 1", "CREATE TABLE `x` (\n"] {
            assert_eq!(filter_create_table(t, &Visibility::none()), t);
        }
        // Names with backquotes, parentheses and commas inside.
        let t = "CREATE TABLE `a``b` (\n  `c,(d` int(11) DEFAULT NULL,\n  `_sprig_x` int(11) DEFAULT NULL,\n  KEY `k(1)` (`c,(d`)\n) ENGINE=InnoDB";
        assert_eq!(
            filter_create_table(t, &Visibility::none()),
            "CREATE TABLE `a``b` (\n  `c,(d` int(11) DEFAULT NULL,\n  KEY `k(1)` (`c,(d`)\n) ENGINE=InnoDB"
        );
    }

    #[test]
    fn create_like_copies_the_visible_definition() {
        let sql = create_like_sql("CREATE TABLE `t9999_d1_g1_s1`.`o2` ", ORDERS).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE `t9999_d1_g1_s1`.`o2` (
  `id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert!(create_like_sql("CREATE TABLE x", "CREATE VIEW v AS select 1").is_none());
    }

    #[test]
    fn index_rules() {
        let v = Visibility::for_persona(Persona::MariaDb);
        assert!(v.hides_index("sprig_auto_00000000", ["a"]));
        assert!(v.hides_index("k", ["a", "_sprig_rk"]));
        assert!(v.hides_index("PRIMARY", ["my_row_id"]));
        assert!(!v.hides_index("k", ["a"]));
        assert!(v.hides_object("__SPRIG_x"));
        assert!(!v.hides_object("_sprig_x"));
    }
}
