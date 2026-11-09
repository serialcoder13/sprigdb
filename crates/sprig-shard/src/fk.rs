//! Foreign keys across table groups (chapter 19, "Foreign keys across
//! groups"): the catalog's `foreign_keys` rows and the helpers the DDL
//! path uses to fill them.
//!
//! A foreign key whose two ends sit on the same shard (one group, the child
//! following its parent's key) stays in MariaDB (`native`). One that
//! crosses groups is checked by the gateway (`gateway`), and one whose
//! parent is a reference table (`reference`): the shard copies of the child
//! table are created without it ([`strip_foreign_keys`]), and the router
//! (`sprig-router::fk`) locks the parent row before a child write and
//! probes the children before a parent delete.

use sprig_common::Result;

use crate::lookup::LookupId;

/// `ON DELETE` and `ON UPDATE` actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FkAction {
    /// `RESTRICT`.
    Restrict,
    /// `NO ACTION` (InnoDB checks it at once, like `RESTRICT`).
    NoAction,
    /// `CASCADE`.
    Cascade,
    /// `SET NULL`.
    SetNull,
}

impl FkAction {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Restrict => "restrict",
            Self::NoAction => "no_action",
            Self::Cascade => "cascade",
            Self::SetNull => "set_null",
        }
    }

    /// From the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        [Self::Restrict, Self::NoAction, Self::Cascade, Self::SetNull]
            .into_iter()
            .find(|a| a.as_str() == s)
    }

    /// Whether a referenced row with children is refused (1451).
    #[must_use]
    pub fn restricts(self) -> bool {
        matches!(self, Self::Restrict | Self::NoAction)
    }

    /// The SQL words (`ON DELETE <words>`).
    #[must_use]
    pub fn sql(self) -> &'static str {
        match self {
            Self::Restrict => "RESTRICT",
            Self::NoAction => "NO ACTION",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
        }
    }
}

/// Who keeps a foreign key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FkEnforcement {
    /// MariaDB, inside each shard.
    Native,
    /// The gateway, across groups.
    Gateway,
    /// The gateway; the parent is a reference table.
    Reference,
}

impl FkEnforcement {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Gateway => "gateway",
            Self::Reference => "reference",
        }
    }

    /// From the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        [Self::Native, Self::Gateway, Self::Reference]
            .into_iter()
            .find(|a| a.as_str() == s)
    }
}

/// A foreign key's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FkState {
    /// Existing rows are being checked; writers already check.
    Validating,
    /// Checked by every writer.
    Enforced,
    /// Being dropped; no longer checked.
    Dropping,
}

impl FkState {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validating => "validating",
            Self::Enforced => "enforced",
            Self::Dropping => "dropping",
        }
    }

    /// From the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        [Self::Validating, Self::Enforced, Self::Dropping]
            .into_iter()
            .find(|a| a.as_str() == s)
    }

    /// Whether writers check it.
    #[must_use]
    pub fn checked(self) -> bool {
        matches!(self, Self::Validating | Self::Enforced)
    }
}

/// One `foreign_keys` row (chapter 19).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    /// `fk_id`.
    pub id: u64,
    /// The constraint's name.
    pub name: String,
    /// The child table's object id.
    pub child: u64,
    /// The child's columns, in key order.
    pub child_columns: Vec<String>,
    /// The parent table's object id.
    pub parent: u64,
    /// The parent's referenced columns, matching `child_columns`.
    pub parent_columns: Vec<String>,
    /// `ON DELETE`.
    pub on_delete: FkAction,
    /// `ON UPDATE`.
    pub on_update: FkAction,
    /// Who keeps it.
    pub enforcement: FkEnforcement,
    /// A lookup on the child columns, when one exists: parent-side probes
    /// go only to the shards it names.
    pub child_lookup: Option<LookupId>,
    /// Its state.
    pub state: FkState,
}

/// The `foreign_keys` table of a tenant's catalog.
#[derive(Debug, Clone, Copy, Default)]
pub struct ForeignKeyCatalog;

impl ForeignKeyCatalog {
    /// Inserts `fk`.
    ///
    /// # Errors
    /// MariaDB's errors (1062 for a repeated id or child and name).
    pub async fn insert(&self, c: &mut mysql_async::Conn, fk: &ForeignKey) -> Result<()> {
        use mysql_async::prelude::Queryable;
        c.exec_drop(
            "INSERT INTO foreign_keys (fk_id, name, child_object_id, child_columns,
               parent_object_id, parent_columns, on_delete, on_update, enforcement,
               child_lookup_id, state, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))",
            (
                fk.id,
                &fk.name,
                fk.child,
                serde_json::to_string(&fk.child_columns).unwrap_or_default(),
                fk.parent,
                serde_json::to_string(&fk.parent_columns).unwrap_or_default(),
                fk.on_delete.as_str(),
                fk.on_update.as_str(),
                fk.enforcement.as_str(),
                fk.child_lookup.map(|l| l.0),
                fk.state.as_str(),
            ),
        )
        .await
        .map_err(crate::groups::db_err)
    }

    /// Every row.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn load(&self, c: &mut mysql_async::Conn) -> Result<Vec<ForeignKey>> {
        use mysql_async::prelude::Queryable;
        let rows: Vec<FkTuple> = c
            .query(
                "SELECT fk_id, name, child_object_id, CAST(child_columns AS CHAR),
                        parent_object_id, CAST(parent_columns AS CHAR), on_delete, on_update,
                        enforcement, child_lookup_id, state
                 FROM foreign_keys ORDER BY fk_id",
            )
            .await
            .map_err(crate::groups::db_err)?;
        Ok(rows.into_iter().filter_map(from_tuple).collect())
    }

    /// Deletes row `id`.
    ///
    /// # Errors
    /// MariaDB's errors.
    pub async fn delete(&self, c: &mut mysql_async::Conn, id: u64) -> Result<()> {
        use mysql_async::prelude::Queryable;
        c.exec_drop("DELETE FROM foreign_keys WHERE fk_id = ?", (id,))
            .await
            .map_err(crate::groups::db_err)
    }
}

/// A `foreign_keys` row as `SELECT` returns it (columns as text).
pub type FkTuple = (
    u64,
    String,
    u64,
    Option<String>,
    u64,
    Option<String>,
    String,
    String,
    String,
    Option<u64>,
    String,
);

/// A row read as [`FkTuple`]; `None` when a value is out of range.
#[must_use]
pub fn from_tuple(t: FkTuple) -> Option<ForeignKey> {
    let (id, name, child, cc, parent, pc, del, upd, enf, lookup, state) = t;
    let cols = |s: Option<String>| -> Option<Vec<String>> {
        let v: Vec<String> = serde_json::from_str(s.as_deref()?).ok()?;
        Some(v.into_iter().map(|c| c.to_ascii_lowercase()).collect())
    };
    Some(ForeignKey {
        id,
        name,
        child,
        child_columns: cols(cc)?,
        parent,
        parent_columns: cols(pc)?,
        on_delete: FkAction::parse(&del)?,
        on_update: FkAction::parse(&upd)?,
        enforcement: FkEnforcement::parse(&enf)?,
        child_lookup: lookup.map(LookupId),
        state: FkState::parse(&state)?,
    })
}

/// A `CONSTRAINT ... FOREIGN KEY ... REFERENCES ...` line of `SHOW CREATE
/// TABLE`, with its actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkLine {
    /// The constraint's name.
    pub name: String,
    /// `ON DELETE` (`RESTRICT` when absent, as InnoDB reads it).
    pub on_delete: FkAction,
    /// `ON UPDATE`.
    pub on_update: FkAction,
}

/// The name and actions of a constraint line, `None` for other lines.
#[must_use]
pub fn parse_line(line: &str) -> Option<FkLine> {
    let l = line.trim().trim_end_matches(',');
    let rest = l.strip_prefix("CONSTRAINT `")?;
    let end = rest.find('`')?;
    let name = rest[..end].to_owned();
    let upper = rest.to_ascii_uppercase();
    upper.find(" FOREIGN KEY ")?;
    let action = |kw: &str| -> FkAction {
        let Some(i) = upper.find(kw) else {
            return FkAction::Restrict;
        };
        let tail = &upper[i + kw.len()..];
        if tail.starts_with("CASCADE") {
            FkAction::Cascade
        } else if tail.starts_with("SET NULL") {
            FkAction::SetNull
        } else if tail.starts_with("NO ACTION") {
            FkAction::NoAction
        } else {
            FkAction::Restrict
        }
    };
    Some(FkLine {
        name,
        on_delete: action(" ON DELETE "),
        on_update: action(" ON UPDATE "),
    })
}

/// `create_sql` (MariaDB's `SHOW CREATE TABLE` layout) without the named
/// foreign keys: a shard's copy of a child table whose keys the gateway
/// checks. The comma before a removed last item goes too.
#[must_use]
pub fn strip_foreign_keys(create_sql: &str, names: &[&str]) -> String {
    let lines: Vec<&str> = create_sql.lines().collect();
    let mut keep: Vec<String> = Vec::with_capacity(lines.len());
    for line in lines {
        if parse_line(line).is_some_and(|f| names.iter().any(|n| n.eq_ignore_ascii_case(&f.name))) {
            continue;
        }
        if line.trim_start().starts_with(')')
            && let Some(prev) = keep.last_mut()
            && let Some(p) = prev.strip_suffix(',')
        {
            *prev = p.to_owned();
        }
        keep.push(line.to_owned());
    }
    keep.join("\n")
}

/// Who keeps a foreign key from `child` to `parent`, given where they live:
/// `same_shard` when both are members of one group and the child's rows
/// sit with their parent (a parent member through this key, or both keyed
/// by the foreign-key columns), or when the tenant's tables share one
/// shard.
#[must_use]
pub fn enforcement(parent_is_reference: bool, same_shard: bool) -> FkEnforcement {
    if same_shard {
        FkEnforcement::Native
    } else if parent_is_reference {
        FkEnforcement::Reference
    } else {
        FkEnforcement::Gateway
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    const CREATE: &str = "CREATE TABLE `parts` (\n  `cid` int(11) NOT NULL,\n  `sid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`cid`),\n  KEY `sid` (`sid`),\n  CONSTRAINT `fk_sup` FOREIGN KEY (`sid`) REFERENCES `suppliers` (`sid`) ON DELETE SET NULL ON UPDATE CASCADE,\n  CONSTRAINT `fk_c` FOREIGN KEY (`cid`) REFERENCES `customers` (`cid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    #[test]
    fn lines_give_actions() {
        let l = parse_line(
            "  CONSTRAINT `fk_sup` FOREIGN KEY (`sid`) REFERENCES `suppliers` (`sid`) ON DELETE SET NULL ON UPDATE CASCADE,",
        )
        .unwrap();
        assert_eq!(l.name, "fk_sup");
        assert_eq!(l.on_delete, FkAction::SetNull);
        assert_eq!(l.on_update, FkAction::Cascade);
        let l = parse_line("CONSTRAINT `x` FOREIGN KEY (`a`) REFERENCES `p` (`a`)").unwrap();
        assert_eq!(
            (l.on_delete, l.on_update),
            (FkAction::Restrict, FkAction::Restrict)
        );
        assert!(parse_line("  KEY `sid` (`sid`),").is_none());
        assert!(parse_line("CONSTRAINT `c` CHECK (`a` > 0)").is_none());
    }

    #[test]
    fn stripping_keeps_the_rest_valid() {
        let s = strip_foreign_keys(CREATE, &["FK_C"]);
        assert!(s.contains("ON UPDATE CASCADE\n) ENGINE"), "{s}");
        assert!(!s.contains("fk_c"));
        let s = strip_foreign_keys(CREATE, &["fk_c", "fk_sup"]);
        assert!(s.contains("KEY `sid` (`sid`)\n) ENGINE"), "{s}");
    }

    #[test]
    fn spellings_round_trip() {
        for a in [
            FkAction::Restrict,
            FkAction::NoAction,
            FkAction::Cascade,
            FkAction::SetNull,
        ] {
            assert_eq!(FkAction::parse(a.as_str()), Some(a));
        }
        assert!(FkAction::NoAction.restricts());
        assert!(!FkAction::Cascade.restricts());
        assert_eq!(enforcement(true, false), FkEnforcement::Reference);
        assert_eq!(enforcement(false, true), FkEnforcement::Native);
        assert_eq!(enforcement(false, false), FkEnforcement::Gateway);
        let t = (
            1,
            "fk".into(),
            2,
            Some("[\"SID\"]".into()),
            3,
            Some("[\"sid\"]".into()),
            "cascade".into(),
            "restrict".into(),
            "gateway".into(),
            None,
            "enforced".into(),
        );
        let f = from_tuple(t).unwrap();
        assert_eq!(f.child_columns, ["sid"]);
        assert_eq!(f.on_delete, FkAction::Cascade);
        assert!(f.state.checked());
    }
}
