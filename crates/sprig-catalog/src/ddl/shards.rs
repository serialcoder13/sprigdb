//! Schema changes across shards: the text-level pieces the DDL path needs
//! when one statement changes several shard schemas.
//!
//! - [`for_shard`] turns the statement rewritten for the database's
//!   template schema into the same statement for another shard.
//! - [`TableDef`] reads `SHOW CREATE TABLE` into its items (columns, keys,
//!   constraints) and table options; [`TableDef::comparable`] is the form
//!   shards are compared in (no `AUTO_INCREMENT=n`, no automatic
//!   `sprig_auto_*` indexes, which are built shard by shard).
//! - [`undo_alter`] computes the `ALTER TABLE` clauses that turn a shard's
//!   new definition back into its old one, or says the change removed
//!   data (a dropped column), so it can only roll forward.
//! - [`undo_statement`] is the inverse of any one physical statement.
//! - [`with_algorithm`] names the algorithm of an `ALTER TABLE`, `CREATE
//!   INDEX`, so MariaDB refuses rather than silently
//!   choosing a blocking one ([`Method`]).

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sprig_common::names::{SchemaName, quote};

use super::exec::{Effect, drop_sql};
use crate::store::ObjectKind;

// ---- one shard's text ----

/// `sql` (written for `template`) for shard schema `to`: every occurrence
/// of the template's name as an identifier, backquoted or bare, becomes
/// `to`'s. A longer name that merely starts with it (`..._s1` and
/// `..._s12`) is left alone.
#[must_use]
pub fn for_shard(sql: &str, template: &SchemaName, to: &SchemaName) -> String {
    if template == to {
        return sql.to_owned();
    }
    let from = template.to_string();
    let to = to.to_string();
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 8);
    let mut last = 0;
    let mut i = 0;
    while let Some(off) = sql[i..].find(&from) {
        let at = i + off;
        let end = at + from.len();
        let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';
        let before_ok = at == 0 || !ident(bytes[at - 1]);
        let after_ok = end >= bytes.len() || !ident(bytes[end]);
        if before_ok && after_ok {
            out.push_str(&sql[last..at]);
            out.push_str(&to);
            last = end;
        }
        i = end;
    }
    out.push_str(&sql[last..]);
    out
}

// ---- SHOW CREATE TABLE ----

/// What one line of a table definition is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemKind {
    /// A column, by name.
    Column(String),
    /// An index (`PRIMARY` for the primary key), by name.
    Key(String),
    /// A named constraint; `foreign` for a foreign key, else a `CHECK`.
    Constraint {
        /// Its name.
        name: String,
        /// A foreign key.
        foreign: bool,
    },
}

/// One item of a table definition: its kind and its text (no trailing
/// comma).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// What it is.
    pub kind: ItemKind,
    /// The line as `SHOW CREATE TABLE` prints it.
    pub text: String,
}

/// A `SHOW CREATE TABLE` text, split into its parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// `CREATE TABLE `t` (`.
    pub head: String,
    /// Columns, keys and constraints, in order.
    pub items: Vec<Item>,
    /// The table options after the closing parenthesis (and any
    /// partitioning), without `AUTO_INCREMENT=n`.
    pub options: String,
}

/// The first backquoted identifier in `s`, unquoted.
fn first_ident(s: &str) -> Option<String> {
    let start = s.find('`')?;
    let rest = &s[start + 1..];
    let mut name = String::new();
    let mut chars = rest.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '`' {
            if chars.peek() == Some(&'`') {
                chars.next();
                name.push('`');
            } else {
                return Some(name);
            }
        } else {
            name.push(c);
        }
    }
    None
}

fn classify(line: &str) -> Option<ItemKind> {
    let up = line.to_ascii_uppercase();
    if line.starts_with('`') {
        return first_ident(line).map(ItemKind::Column);
    }
    if up.starts_with("PRIMARY KEY") {
        return Some(ItemKind::Key("PRIMARY".to_owned()));
    }
    if up.starts_with("CONSTRAINT") {
        return first_ident(line).map(|name| ItemKind::Constraint {
            name,
            foreign: up.contains(" FOREIGN KEY "),
        });
    }
    for p in [
        "UNIQUE KEY",
        "KEY",
        "INDEX",
        "FULLTEXT KEY",
        "SPATIAL KEY",
        "VECTOR KEY",
        "UNIQUE INDEX",
        "FULLTEXT INDEX",
        "SPATIAL INDEX",
        "VECTOR INDEX",
    ] {
        if up.starts_with(p) && up[p.len()..].starts_with([' ', '`']) {
            return first_ident(&line[p.len()..]).map(ItemKind::Key);
        }
    }
    None
}

/// `AUTO_INCREMENT=n` removed from table options.
fn strip_auto_increment(options: &str) -> String {
    let mut out = Vec::new();
    for w in options.split(' ') {
        if w.len() > 15
            && w[..15].eq_ignore_ascii_case("AUTO_INCREMENT=")
            && w[15..].bytes().all(|b| b.is_ascii_digit())
        {
            continue;
        }
        out.push(w);
    }
    out.join(" ")
}

impl TableDef {
    /// Parses `SHOW CREATE TABLE` output; `None` for text it doesn't
    /// recognise (a view, a sequence, an unknown line).
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let head = lines.next()?.trim_end().to_owned();
        if !head.to_ascii_uppercase().starts_with("CREATE") || !head.ends_with('(') {
            return None;
        }
        let mut items: Vec<Item> = Vec::new();
        let mut options = None;
        let mut rest: Vec<&str> = Vec::new();
        for line in lines.by_ref() {
            let t = line.trim();
            if options.is_none() && t.starts_with(')') {
                options = Some(t[1..].trim().to_owned());
                continue;
            }
            if options.is_some() {
                rest.push(t);
                continue;
            }
            let body = t.strip_suffix(',').unwrap_or(t);
            match classify(body) {
                Some(kind) if line.starts_with("  ") && !line.starts_with("   ") => {
                    items.push(Item {
                        kind,
                        text: body.to_owned(),
                    });
                }
                // A continuation of the previous item (a comment or
                // expression with a line break).
                _ => {
                    let last = items.last_mut()?;
                    if let Some(prev) = last.text.strip_suffix(',') {
                        last.text = prev.to_owned();
                    }
                    last.text.push('\n');
                    last.text.push_str(line.strip_suffix(',').unwrap_or(line));
                }
            }
        }
        let mut options = strip_auto_increment(&options?);
        for r in rest {
            options.push('\n');
            options.push_str(r);
        }
        Some(Self {
            head,
            items,
            options,
        })
    }

    /// The item of `kind`.
    #[must_use]
    pub fn item(&self, kind: &ItemKind) -> Option<&Item> {
        self.items.iter().find(|i| same_item(&i.kind, kind))
    }

    /// The definition as text, items in order.
    #[must_use]
    pub fn render(&self) -> String {
        let mut s = self.head.clone();
        s.push('\n');
        let n = self.items.len();
        for (i, it) in self.items.iter().enumerate() {
            s.push_str("  ");
            s.push_str(&it.text);
            if i + 1 < n {
                s.push(',');
            }
            s.push('\n');
        }
        s.push(')');
        if !self.options.is_empty() {
            s.push(' ');
            s.push_str(&self.options);
        }
        s
    }

    /// The form shards are compared in: no `AUTO_INCREMENT=n` and no
    /// `sprig_auto_*` index (automatic indexes are built shard by shard).
    #[must_use]
    pub fn comparable(&self) -> String {
        let mut d = self.clone();
        d.items.retain(|i| !automatic(&i.kind));
        d.render()
    }
}

/// `SHOW CREATE` text in the form shards are compared in: tables through
/// [`TableDef::comparable`], other objects as they are.
#[must_use]
pub fn comparable(text: &str) -> String {
    TableDef::parse(text).map_or_else(|| text.to_owned(), |d| d.comparable())
}

fn same_item(a: &ItemKind, b: &ItemKind) -> bool {
    match (a, b) {
        (ItemKind::Column(x), ItemKind::Column(y))
        | (ItemKind::Key(x), ItemKind::Key(y))
        | (ItemKind::Constraint { name: x, .. }, ItemKind::Constraint { name: y, .. }) => {
            x.eq_ignore_ascii_case(y)
        }
        _ => false,
    }
}

/// How to turn a changed table back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterUndo {
    /// Nothing differs.
    Nothing,
    /// These `ALTER TABLE` clauses, comma-separated, undo the change.
    Clauses(String),
    /// The change removed a column (its data is gone on the shards that
    /// ran it), or changed partitioning: it can only roll forward.
    Forward,
}

/// The clauses that turn table `now` back into `before` (see
/// [`AlterUndo`]). Order: drop the new constraints and keys, drop the new
/// columns, restore changed columns, re-add removed keys and constraints,
/// then the table options.
#[must_use]
pub fn undo_alter(before: &TableDef, now: &TableDef) -> AlterUndo {
    let mut drops = Vec::new();
    let mut modifies = Vec::new();
    let mut adds = Vec::new();
    for it in before.items.iter().filter(|i| !automatic(&i.kind)) {
        match now.item(&it.kind) {
            None if matches!(it.kind, ItemKind::Column(_)) => return AlterUndo::Forward,
            None => adds.push(add_clause(it)),
            Some(n) if n.text == it.text => {}
            Some(n) => {
                if let ItemKind::Column(_) = &it.kind {
                    modifies.push(format!("MODIFY COLUMN {}", it.text));
                } else {
                    drops.push(drop_clause(&n.kind));
                    adds.push(add_clause(it));
                }
            }
        }
    }
    let mut new_cols = Vec::new();
    for n in now.items.iter().filter(|i| !automatic(&i.kind)) {
        if before.item(&n.kind).is_none() {
            match &n.kind {
                ItemKind::Column(c) => new_cols.push(format!("DROP COLUMN {}", quote(c))),
                k => drops.push(drop_clause(k)),
            }
        }
    }
    let mut clauses: Vec<String> = drops;
    clauses.extend(new_cols);
    clauses.extend(modifies);
    clauses.extend(adds);
    if before.options != now.options {
        let partitioned = |o: &str| o.to_ascii_uppercase().contains("PARTITION");
        if partitioned(&before.options) || partitioned(&now.options) {
            return AlterUndo::Forward;
        }
        let have = option_tokens(&now.options);
        let mut opts: Vec<String> = option_tokens(&before.options)
            .into_iter()
            .filter(|t| !have.contains(t))
            .collect();
        let has_comment = |o: &str| o.to_ascii_uppercase().contains("COMMENT=");
        if has_comment(&now.options) && !has_comment(&before.options) {
            opts.push("COMMENT=''".to_owned());
        }
        if !opts.is_empty() {
            clauses.push(opts.join(" "));
        }
    }
    if clauses.is_empty() {
        AlterUndo::Nothing
    } else {
        AlterUndo::Clauses(clauses.join(", "))
    }
}

/// Table options split at spaces outside quotes (`COMMENT='a b'` is one).
fn option_tokens(options: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in options.chars() {
        if c == '\'' {
            quoted = !quoted;
        }
        if c.is_whitespace() && !quoted {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// An automatic index: built shard by shard, never part of a
/// schema change.
fn automatic(k: &ItemKind) -> bool {
    matches!(k, ItemKind::Key(n) if n.to_ascii_lowercase().starts_with("sprig_auto_"))
}

fn drop_clause(k: &ItemKind) -> String {
    match k {
        ItemKind::Column(c) => format!("DROP COLUMN {}", quote(c)),
        ItemKind::Key(n) if n.eq_ignore_ascii_case("PRIMARY") => "DROP PRIMARY KEY".to_owned(),
        ItemKind::Key(n) => format!("DROP KEY {}", quote(n)),
        ItemKind::Constraint {
            name,
            foreign: true,
        } => format!("DROP FOREIGN KEY {}", quote(name)),
        ItemKind::Constraint { name, .. } => format!("DROP CONSTRAINT {}", quote(name)),
    }
}

fn add_clause(it: &Item) -> String {
    match it.kind {
        ItemKind::Column(_) => format!("ADD COLUMN {}", it.text),
        _ => format!("ADD {}", it.text),
    }
}

// ---- inverse statements ----

/// What undoing one shard's statement takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Undo {
    /// Run this statement in the shard's schema.
    Sql(String),
    /// Nothing changed that needs undoing.
    Nothing,
    /// The statement removed something: it only rolls forward.
    Forward,
}

/// `CREATE ...` from `SHOW CREATE` as `CREATE OR REPLACE ...`.
fn or_replace(text: &str) -> String {
    let t = text.trim_start();
    if t.len() >= 6 && t[..6].eq_ignore_ascii_case("CREATE") {
        let rest = &t[6..];
        if rest
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("OR REPLACE")
        {
            return t.to_owned();
        }
        return format!("CREATE OR REPLACE{rest}");
    }
    t.to_owned()
}

/// The inverse of `effect` run in shard `schema`: `before` is the object's
/// `SHOW CREATE` text on a shard that didn't run it (`None` when it didn't
/// exist), written for schema `before_schema`; `now` is its text on a
/// shard that did.
#[must_use]
pub fn undo_statement(
    effect: &Effect,
    schema: &SchemaName,
    before: Option<(&str, &SchemaName)>,
    now: Option<&str>,
) -> Undo {
    match effect {
        Effect::Create { kind, name } => match before {
            None => Undo::Sql(drop_sql(schema, *kind, std::slice::from_ref(name))),
            Some((text, from)) if *kind == ObjectKind::Table => {
                // CREATE OR REPLACE TABLE: the old rows are gone.
                let _ = (text, from);
                Undo::Forward
            }
            Some((text, from)) => Undo::Sql(or_replace(&for_shard(text, from, schema))),
        },
        Effect::Alter { kind, name } => {
            let Some((text, from)) = before else {
                return Undo::Nothing;
            };
            if *kind != ObjectKind::Table {
                return Undo::Sql(or_replace(&for_shard(text, from, schema)));
            }
            let (Some(b), Some(n)) = (TableDef::parse(text), now.and_then(TableDef::parse)) else {
                return Undo::Forward;
            };
            match undo_alter(&b, &n) {
                AlterUndo::Nothing => Undo::Nothing,
                AlterUndo::Forward => Undo::Forward,
                AlterUndo::Clauses(c) => Undo::Sql(format!(
                    "ALTER TABLE {}.{} {}",
                    quote(&schema.to_string()),
                    quote(name),
                    for_shard(&c, from, schema)
                )),
            }
        }
        Effect::Rename { moves } => {
            let back: Vec<String> = moves
                .iter()
                .rev()
                .map(|(fs, fname, ts, tname)| {
                    format!(
                        "{}.{} TO {}.{}",
                        quote(&ts.to_string()),
                        quote(tname),
                        quote(&fs.to_string()),
                        quote(fname)
                    )
                })
                .collect();
            Undo::Sql(format!("RENAME TABLE {}", back.join(", ")))
        }
        Effect::Drop { .. } | Effect::DropSchema => Undo::Forward,
        Effect::Truncate { .. }
        | Effect::CreateSchema
        | Effect::AlterSchema
        | Effect::Restore { .. } => Undo::Nothing,
    }
}

// ---- algorithms ----

/// How MariaDB runs a table change on a shard, tried in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// `ALGORITHM=INSTANT`: metadata only.
    Instant,
    /// `ALGORITHM=NOCOPY, LOCK=NONE`.
    NoCopy,
    /// `ALGORITHM=INPLACE, LOCK=NONE`.
    Inplace,
    /// `ALGORITHM=COPY, LOCK=NONE` (MariaDB 11.2 and later).
    CopyOnline,
    /// The statement as written: MariaDB chooses, and may block writes for
    /// the duration on that shard (copy-and-swap through the mover is not
    /// wired into DDL jobs yet).
    AsWritten,
}

impl Method {
    /// Every method, in the order tried.
    pub const ORDER: [Method; 5] = [
        Method::Instant,
        Method::NoCopy,
        Method::Inplace,
        Method::CopyOnline,
        Method::AsWritten,
    ];

    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Method::Instant => "instant",
            Method::NoCopy => "nocopy",
            Method::Inplace => "inplace",
            Method::CopyOnline => "copy_online",
            Method::AsWritten => "as_written",
        }
    }

    fn clause(self) -> Option<(&'static str, bool)> {
        match self {
            Method::Instant => Some(("INSTANT", false)),
            Method::NoCopy => Some(("NOCOPY", true)),
            Method::Inplace => Some(("INPLACE", true)),
            Method::CopyOnline => Some(("COPY", true)),
            Method::AsWritten => None,
        }
    }
}

/// MariaDB refused the named algorithm or lock (1845, 1846): try the next
/// method.
#[must_use]
pub fn method_refused(errno: u16) -> bool {
    matches!(errno, 1845 | 1846)
}

fn words(sql: &str) -> impl Iterator<Item = String> + '_ {
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_uppercase)
}

/// Whether `sql` is a table change whose algorithm SprigDB names: `ALTER
/// TABLE` or `CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX`, without
/// its own `ALGORITHM` or `LOCK`, and without comments (a clause appended
/// after a line comment would be lost).
#[must_use]
pub fn choosable(sql: &str) -> bool {
    if sql.contains("--") || sql.contains('#') || sql.contains("/*") {
        return false;
    }
    let w: Vec<String> = words(sql).take(4).collect();
    let head = |i: usize| w.get(i).map_or("", String::as_str);
    let alter =
        head(0) == "ALTER" && (head(1) == "TABLE" || (head(1) == "ONLINE" && head(2) == "TABLE"));
    // MariaDB's `DROP INDEX` takes no algorithm (1064); it is `NOCOPY`
    // anyway.
    let index =
        head(0) == "CREATE" && (head(1) == "INDEX" || (head(2) == "INDEX" && head(1) != "OR"));
    (alter || index)
        && !words(sql).any(|w| w == "ALGORITHM" || w == "LOCK" || w == "RENAME" || w == "PARTITION")
}

/// `sql` with `method`'s `ALGORITHM` (and `LOCK=NONE`) appended; `sql` as
/// it is for [`Method::AsWritten`] or a statement that isn't
/// [`choosable`].
#[must_use]
pub fn with_algorithm(sql: &str, method: Method) -> String {
    let base = sql.trim_end().trim_end_matches(';').trim_end();
    let Some((alg, lock)) = method.clause() else {
        return base.to_owned();
    };
    if !choosable(base) {
        return base.to_owned();
    }
    let alter = words(base).next().is_some_and(|w| w == "ALTER");
    let mut out = base.to_owned();
    if alter {
        let _ = write!(out, ", ALGORITHM={alg}");
        if lock {
            out.push_str(", LOCK=NONE");
        }
    } else {
        let _ = write!(out, " ALGORITHM={alg}");
        if lock {
            out.push_str(" LOCK=NONE");
        }
    }
    out
}

/// The methods to try for `sql`, starting from the one an earlier shard
/// of the same job used (`hint`): a shard never needs a cheaper method
/// than one that MariaDB already refused on another.
#[must_use]
pub fn methods(sql: &str, hint: Option<Method>) -> Vec<Method> {
    if !choosable(sql) {
        return vec![Method::AsWritten];
    }
    let from = hint
        .and_then(|h| Method::ORDER.iter().position(|m| *m == h))
        .unwrap_or(0);
    Method::ORDER[from..].to_vec()
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};

    use super::*;

    fn s(n: u32) -> SchemaName {
        SchemaName::shard(TenantId(42), DbId(1), GroupId(1), ShardId(n))
    }

    const T0: &str = "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n  `qty` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `by_qty` (`qty`),\n  KEY `sprig_auto_0a1b2c3d` (`qty`,`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

    #[test]
    fn for_shard_replaces_whole_names_only() {
        let sql = "ALTER TABLE `t42_d1_g1_s1`.`o` ADD c INT, t42_d1_g1_s1.x, `t42_d1_g1_s12`.y";
        assert_eq!(
            for_shard(sql, &s(1), &s(3)),
            "ALTER TABLE `t42_d1_g1_s3`.`o` ADD c INT, t42_d1_g1_s3.x, `t42_d1_g1_s12`.y"
        );
        assert_eq!(for_shard(sql, &s(1), &s(1)), sql);
    }

    #[test]
    fn parses_show_create_table() {
        let d = TableDef::parse(T0).unwrap();
        assert_eq!(d.items.len(), 5);
        assert_eq!(d.items[0].kind, ItemKind::Column("id".into()));
        assert_eq!(d.items[2].kind, ItemKind::Key("PRIMARY".into()));
        assert_eq!(d.items[3].kind, ItemKind::Key("by_qty".into()));
        assert!(!d.options.contains("AUTO_INCREMENT"));
        assert!(!d.comparable().contains("sprig_auto_"));
        let mut other = T0.replace("AUTO_INCREMENT=17", "AUTO_INCREMENT=99");
        other = other.replace(",\n  KEY `sprig_auto_0a1b2c3d` (`qty`,`id`)", "");
        assert_eq!(comparable(T0), comparable(&other));
    }

    #[test]
    fn checks_generated_columns_and_foreign_keys_parse() {
        let t = "CREATE TABLE `c` (\n  `a` int(11) NOT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` * 2) VIRTUAL,\n  `p` int(11) DEFAULT NULL,\n  UNIQUE KEY `u` (`b`),\n  KEY `fk1` (`p`),\n  CONSTRAINT `fk1` FOREIGN KEY (`p`) REFERENCES `par` (`id`),\n  CONSTRAINT `pos` CHECK (`a` > 0)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        let d = TableDef::parse(t).unwrap();
        assert_eq!(d.items.len(), 7);
        assert_eq!(
            d.items[5].kind,
            ItemKind::Constraint {
                name: "fk1".into(),
                foreign: true
            }
        );
        assert_eq!(
            d.items[6].kind,
            ItemKind::Constraint {
                name: "pos".into(),
                foreign: false
            }
        );
        assert_eq!(d.render(), t);
    }

    #[test]
    fn undo_of_additions_drops_them() {
        let before = TableDef::parse(T0).unwrap();
        let now = TableDef::parse(
            "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n  `qty` int(11) DEFAULT NULL,\n  `note` varchar(200) DEFAULT NULL,\n  `q2` int(11) GENERATED ALWAYS AS (`qty` * 2) VIRTUAL,\n  PRIMARY KEY (`id`),\n  KEY `by_qty` (`qty`),\n  KEY `by_note` (`note`),\n  CONSTRAINT `qty_pos` CHECK (`qty` > 0)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci COMMENT='x'",
        )
        .unwrap();
        // `sprig_auto_` indexes are left alone.
        let AlterUndo::Clauses(c) = undo_alter(&before, &now) else {
            panic!()
        };
        assert_eq!(
            c,
            "DROP KEY `by_note`, DROP CONSTRAINT `qty_pos`, DROP COLUMN `note`, DROP COLUMN `q2`, \
             COMMENT=''"
        );
    }

    #[test]
    fn undo_of_a_type_change_restores_the_column_and_a_drop_rolls_forward() {
        let before = TableDef::parse(T0).unwrap();
        let widened = TableDef::parse(&T0.replace("`qty` int(11)", "`qty` bigint(20)")).unwrap();
        assert_eq!(
            undo_alter(&before, &widened),
            AlterUndo::Clauses("MODIFY COLUMN `qty` int(11) DEFAULT NULL".into())
        );
        let dropped = TableDef::parse(
            "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
        )
        .unwrap();
        assert_eq!(undo_alter(&before, &dropped), AlterUndo::Forward);
        assert_eq!(undo_alter(&before, &before), AlterUndo::Nothing);
    }

    #[test]
    fn undo_statements() {
        let create = Effect::Create {
            kind: ObjectKind::Procedure,
            name: "p".into(),
        };
        assert_eq!(
            undo_statement(&create, &s(2), None, None),
            Undo::Sql("DROP PROCEDURE IF EXISTS `t42_d1_g1_s2`.`p`".into())
        );
        let pre = "CREATE DEFINER=`t42_u1`@`%` PROCEDURE `p`()\nSELECT 1 FROM t42_d1_g1_s1.x";
        assert_eq!(
            undo_statement(&create, &s(2), Some((pre, &s(1))), None),
            Undo::Sql(
                "CREATE OR REPLACE DEFINER=`t42_u1`@`%` PROCEDURE `p`()\nSELECT 1 FROM t42_d1_g1_s2.x"
                    .into()
            )
        );
        let alter = Effect::Alter {
            kind: ObjectKind::Table,
            name: "orders".into(),
        };
        let now = T0.replace(
            "  PRIMARY KEY",
            "  `note` varchar(9) DEFAULT NULL,\n  PRIMARY KEY",
        );
        assert_eq!(
            undo_statement(&alter, &s(2), Some((T0, &s(1))), Some(&now)),
            Undo::Sql("ALTER TABLE `t42_d1_g1_s2`.`orders` DROP COLUMN `note`".into())
        );
        let rename = Effect::Rename {
            moves: vec![(s(2), "a".into(), s(2), "b".into())],
        };
        assert_eq!(
            undo_statement(&rename, &s(2), None, None),
            Undo::Sql("RENAME TABLE `t42_d1_g1_s2`.`b` TO `t42_d1_g1_s2`.`a`".into())
        );
        assert_eq!(
            undo_statement(
                &Effect::Drop {
                    kind: ObjectKind::Table,
                    names: vec!["x".into()]
                },
                &s(2),
                None,
                None
            ),
            Undo::Forward
        );
    }

    #[test]
    fn algorithms_are_named() {
        let a = "ALTER TABLE `t42_d1_g1_s1`.`o` ADD COLUMN c INT;";
        assert!(choosable(a));
        assert_eq!(
            with_algorithm(a, Method::Instant),
            "ALTER TABLE `t42_d1_g1_s1`.`o` ADD COLUMN c INT, ALGORITHM=INSTANT"
        );
        assert_eq!(
            with_algorithm(a, Method::NoCopy),
            "ALTER TABLE `t42_d1_g1_s1`.`o` ADD COLUMN c INT, ALGORITHM=NOCOPY, LOCK=NONE"
        );
        assert_eq!(
            with_algorithm("CREATE INDEX i ON o (c)", Method::Inplace),
            "CREATE INDEX i ON o (c) ALGORITHM=INPLACE LOCK=NONE"
        );
        assert!(choosable("create unique index i on o (c)"));
        assert!(!choosable("ALTER TABLE o ADD c INT, ALGORITHM=COPY"));
        assert!(!choosable("ALTER TABLE o ADD c INT -- note"));
        assert!(!choosable("CREATE PROCEDURE p() SELECT 1"));
        assert!(!choosable("DROP INDEX i ON o"));
        assert_eq!(
            with_algorithm("ALTER TABLE o FORCE", Method::AsWritten),
            "ALTER TABLE o FORCE"
        );
        assert_eq!(
            methods(a, Some(Method::Inplace)).first(),
            Some(&Method::Inplace)
        );
        assert_eq!(methods("DROP TABLE x", None), vec![Method::AsWritten]);
        assert!(method_refused(1846) && !method_refused(4025));
    }
}
