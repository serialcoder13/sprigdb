//! Children receive the tables their parent shares (Kiran's decisions 1
//! and 2, 11 November 2026; decision IG3).
//!
//! A child tenant no longer declares the tables it inherits. Before a
//! statement of a tenant whose parent shares it anything, the gateway
//! compares the two catalogs ([`wanted`]) and makes the child match, as
//! the provisioning account through the DDL engine, top tenant first (so
//! `b` is brought up to date before `c`):
//!
//! ```text
//! a table the parent shares (a share of it, or of a table of its
//!   foreign-key group) that the child lacks → created with the parent's
//!   physical definition (layer columns and keys included), registered
//!   inheritable with the parent's key, AUTO_INCREMENT column and links
//! a column the parent's table has and the child's lacks → added
//! links and the AUTO_INCREMENT column → copied
//! ```
//!
//! The child's own additions stay its own and flow on to its children the
//! same way: each level's schema is its parent's plus its own. A table the
//! child declared itself (before this change) is kept and gains the
//! parent's missing columns.

use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;

use crate::model::{InheritableObject, Link};
use crate::plan::state_of;

/// One table a child must hold.
#[derive(Debug, Clone)]
pub struct Want {
    /// The logical database (same name in both tenants).
    pub db: String,
    /// The table.
    pub table: String,
    /// The parent's object.
    pub parent: Arc<InheritableObject>,
    /// The child's object, if it is already inheritable there.
    pub child: Option<Arc<InheritableObject>>,
    /// The child has a table of that name that isn't inheritable (left
    /// alone: it shadows the inherited one, as before).
    pub plain: bool,
    /// The parent's columns the child's table lacks.
    pub missing: Vec<String>,
    /// The child's object needs the parent's links or `AUTO_INCREMENT`
    /// column recorded.
    pub ext: bool,
}

impl Want {
    /// Whether the child is already up to date for this table.
    #[must_use]
    pub fn done(&self) -> bool {
        self.plain || (self.child.is_some() && self.missing.is_empty() && !self.ext)
    }
}

/// The child's links: the parent's, plus its own (by name).
#[must_use]
pub fn merged_links(parent: &[Link], child: &[Link]) -> Vec<Link> {
    let mut out = parent.to_vec();
    for l in child {
        if !out.iter().any(|x| x.name.eq_ignore_ascii_case(&l.name)) {
            out.push(l.clone());
        }
    }
    out
}

/// What `child` (a direct child tenant of `parent`) must hold.
#[must_use]
pub fn wanted(parent: &TenantCatalog, child: &TenantCatalog, lctn: bool) -> Vec<Want> {
    let Some(ps) = state_of(parent) else {
        return Vec::new();
    };
    let cs = state_of(child);
    let mut out = Vec::new();
    for po in ps.objects() {
        if ps.tenant_edge(po.object_id, child.tenant).is_none() {
            continue;
        }
        let Some(db) = parent.databases.get(po.db).map(|d| d.name.clone()) else {
            continue;
        };
        let cd = child.database(&database_key(&db, lctn));
        let co = cd
            .and_then(|d| cs.and_then(|s| s.object_named_ci(d.db_id, &po.table)))
            .cloned();
        let plain = co.is_none()
            && cd.is_some_and(|d| {
                child
                    .object(
                        d.db_id,
                        sprig_catalog::store::Namespace::Relation,
                        &sprig_catalog::ddl::object_key(
                            sprig_catalog::store::ObjectKind::Table,
                            &po.table,
                            lctn,
                        ),
                    )
                    .is_some()
            });
        let (missing, ext) = match &co {
            Some(c) => (
                po.columns
                    .iter()
                    .filter(|x| c.column(x).is_none())
                    .cloned()
                    .collect(),
                merged_links(&po.links, &c.links) != c.links
                    || (c.auto.is_none() && po.auto.is_some()),
            ),
            None => (Vec::new(), true),
        };
        out.push(Want {
            db,
            table: po.table.clone(),
            parent: Arc::clone(po),
            child: co,
            plain,
            missing,
            ext,
        });
    }
    out
}

fn is_layer_column(name: &str) -> bool {
    matches!(
        name,
        "_sprig_space" | "_sprig_state" | "_sprig_ovr" | "_sprig_rk"
    )
}

/// The name a `SHOW CREATE TABLE` line defines (a column or a key).
fn line_name(line: &str) -> Option<&str> {
    let t = line.trim_start();
    let t = t
        .strip_prefix("UNIQUE KEY ")
        .or_else(|| t.strip_prefix("KEY "))
        .or_else(|| t.strip_prefix("FULLTEXT KEY "))
        .or_else(|| t.strip_prefix("SPATIAL KEY "))
        .unwrap_or(t);
    let t = t.strip_prefix('`')?;
    t.find('`').map(|e| &t[..e])
}

/// The child's `CREATE TABLE` from the parent's physical `SHOW CREATE
/// TABLE` text: named `target` (already quoted), without foreign keys,
/// SprigDB's own extra columns and indexes (automatic indexes, field
/// columns) and the `AUTO_INCREMENT=` counter.
#[must_use]
pub fn child_create(show_create: &str, target: &str) -> String {
    let lines: Vec<&str> = show_create.lines().collect();
    if lines.len() < 2 {
        return show_create.to_owned();
    }
    let mut body: Vec<String> = Vec::new();
    for l in &lines[1..lines.len() - 1] {
        let t = l.trim_start();
        if t.starts_with("CONSTRAINT") && t.contains("FOREIGN KEY") {
            continue;
        }
        if let Some(n) = line_name(l)
            && n.starts_with("_sprig_")
            && !is_layer_column(n)
            && n != "_sprig_layer"
        {
            continue;
        }
        body.push(l.trim_end().trim_end_matches(',').to_owned());
    }
    let last = lines[lines.len() - 1];
    let tail: String = last
        .split(' ')
        .filter(|w| !w.starts_with("AUTO_INCREMENT="))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n{}\n{tail}",
        body.join(",\n")
    )
}

/// Column `col`'s definition in a `SHOW CREATE TABLE` text (for `ADD
/// COLUMN`).
#[must_use]
pub fn column_def(show_create: &str, col: &str) -> Option<String> {
    show_create
        .lines()
        .find(|l| {
            line_name(l).is_some_and(|n| n.eq_ignore_ascii_case(col)) && {
                let t = l.trim_start();
                !t.starts_with("KEY") && !t.starts_with("UNIQUE")
            }
        })
        .map(|l| l.trim().trim_end_matches(',').to_owned())
}

/// The type and nullability of a `SHOW CREATE TABLE` column line
/// (`` `name` varchar(40) DEFAULT NULL ``): `("varchar(40)", true)`.
fn def_type(def: &str) -> Option<(String, bool)> {
    let t = def.trim_start().strip_prefix('`')?;
    let rest = t[t.find('`')? + 1..].trim_start();
    let mut depth = 0i32;
    let mut quoted = false;
    let mut end = rest.len();
    for (i, ch) in rest.char_indices() {
        match ch {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth -= 1,
            ' ' if !quoted && depth == 0 => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    let mut ty = rest[..end].to_owned();
    let tail = &rest[end..];
    for w in ["unsigned", "zerofill"] {
        if tail.split_whitespace().any(|x| x.eq_ignore_ascii_case(w)) {
            ty.push(' ');
            ty.push_str(w);
        }
    }
    let upper = tail.to_ascii_uppercase();
    Some((ty, !upper.contains("NOT NULL")))
}

/// `MODIFY COLUMN` clauses that give the child's table the parent's wider
/// types for `columns` (both tables have them): a type the parent widened
/// (`VARCHAR(20)` → `VARCHAR(40)`, `INT` → `BIGINT`, ...) or a column it
/// made nullable, from the two `SHOW CREATE TABLE` texts. A column the
/// child widened further itself, or whose types aren't comparable, is left
/// alone.
#[must_use]
pub fn widened(parent_show: &str, child_show: &str, columns: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for c in columns {
        let (Some(pd), Some(cd)) = (column_def(parent_show, c), column_def(child_show, c)) else {
            continue;
        };
        let (Some((pt, pn)), Some((ct, cn))) = (def_type(&pd), def_type(&cd)) else {
            continue;
        };
        let wider = !pt.eq_ignore_ascii_case(&ct) && crate::guard::widens(&ct, &pt);
        let same = pt.eq_ignore_ascii_case(&ct);
        let nulls = pn && !cn;
        if (wider || (same && nulls)) && (pn || !cn) {
            out.push(format!("MODIFY COLUMN {pd}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widened_types_reach_the_child() {
        let parent = "CREATE TABLE `t1` (\n  `id` bigint(20) NOT NULL,\n  `name` varchar(40) \
                      DEFAULT NULL,\n  `qty` int(10) unsigned NOT NULL DEFAULT 0,\n  `code` \
                      char(4) DEFAULT NULL,\n  `mine` varchar(10) DEFAULT NULL,\n  `e` \
                      enum('a','b','c') DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB";
        let child = "CREATE TABLE `t1` (\n  `id` int(11) NOT NULL,\n  `name` varchar(20) DEFAULT \
                     NULL,\n  `qty` int(10) unsigned NOT NULL DEFAULT 0,\n  `code` char(4) NOT \
                     NULL,\n  `mine` varchar(80) DEFAULT NULL,\n  `e` enum('a','b') DEFAULT \
                     NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB";
        let cols: Vec<String> = ["id", "name", "qty", "code", "mine", "e"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(
            widened(parent, child, &cols),
            vec![
                "MODIFY COLUMN `id` bigint(20) NOT NULL".to_owned(),
                "MODIFY COLUMN `name` varchar(40) DEFAULT NULL".to_owned(),
                "MODIFY COLUMN `code` char(4) DEFAULT NULL".to_owned(),
                "MODIFY COLUMN `e` enum('a','b','c') DEFAULT NULL".to_owned(),
            ]
        );
        // Nothing to do when they agree, or the child is wider.
        assert!(widened(child, child, &cols).is_empty());
        assert_eq!(
            def_type("`p` decimal(10,2) unsigned NOT NULL"),
            Some(("decimal(10,2) unsigned".to_owned(), false))
        );
    }

    const SHOW: &str = "CREATE TABLE `t1` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `t2_id` \
                        int(11) DEFAULT NULL,\n  `name` varchar(20) DEFAULT NULL,\n  \
                        `_sprig_space` bigint(20) unsigned NOT NULL DEFAULT 1 INVISIBLE,\n  \
                        `_sprig_state` enum('own','ovr','hide','orphan') NOT NULL DEFAULT 'own' \
                        INVISIBLE,\n  `_sprig_ovr` bigint(20) unsigned NOT NULL DEFAULT \
                        18446744073709551615 INVISIBLE,\n  `_sprig_p_x` longblob GENERATED \
                        ALWAYS AS (1) VIRTUAL INVISIBLE,\n  PRIMARY KEY (`id`,`_sprig_space`),\n  \
                        KEY `_sprig_layer` (`_sprig_space`,`_sprig_state`,`id`),\n  KEY \
                        `_sprig_ix_1` (`name`),\n  KEY `k_name` (`name`)\n) ENGINE=InnoDB \
                        AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4";

    #[test]
    fn the_childs_table() {
        let c = child_create(SHOW, "`t9_d1_g1_s1`.`t1`");
        assert_eq!(
            c,
            "CREATE TABLE IF NOT EXISTS `t9_d1_g1_s1`.`t1` (\n  `id` int(11) NOT NULL \
             AUTO_INCREMENT,\n  `t2_id` int(11) DEFAULT NULL,\n  `name` varchar(20) DEFAULT \
             NULL,\n  `_sprig_space` bigint(20) unsigned NOT NULL DEFAULT 1 INVISIBLE,\n  \
             `_sprig_state` enum('own','ovr','hide','orphan') NOT NULL DEFAULT 'own' \
             INVISIBLE,\n  `_sprig_ovr` bigint(20) unsigned NOT NULL DEFAULT \
             18446744073709551615 INVISIBLE,\n  PRIMARY KEY (`id`,`_sprig_space`),\n  KEY \
             `_sprig_layer` (`_sprig_space`,`_sprig_state`,`id`),\n  KEY `k_name` \
             (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert_eq!(
            column_def(SHOW, "name").as_deref(),
            Some("`name` varchar(20) DEFAULT NULL")
        );
        assert_eq!(column_def(SHOW, "k_name"), None);
    }

    #[test]
    fn links_merge_by_name() {
        let l = |n: &str| Link {
            name: n.into(),
            columns: vec!["a".into()],
            ref_table: "t".into(),
            ref_columns: vec!["id".into()],
        };
        assert_eq!(
            merged_links(&[l("f1")], &[l("F1"), l("f2")]),
            vec![l("f1"), l("f2")]
        );
    }
}
