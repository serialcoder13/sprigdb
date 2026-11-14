//! Typed DDL targets: what a DDL statement does to the catalog, as the
//! rewrite read it (the gateway side of catalog DDL).
//!
//! Both name finders fill [`StatementInfo::ddl`](super::StatementInfo::ddl)
//! for every statement the catalog's DDL engine handles:
//!
//! | Statement | Target |
//! |---|---|
//! | `CREATE DATABASE` / `SCHEMA` | [`DdlTarget::CreateDatabase`] |
//! | `ALTER DATABASE` / `SCHEMA` | [`DdlTarget::AlterDatabase`] |
//! | `DROP DATABASE` / `SCHEMA` | [`DdlTarget::DropDatabase`] |
//! | `CREATE [OR REPLACE] [TEMPORARY] TABLE`, `VIEW`, `SEQUENCE`, `PROCEDURE`, `FUNCTION`, `TRIGGER`, `EVENT` | [`DdlTarget::CreateObject`] |
//! | `ALTER TABLE`, `VIEW`, `SEQUENCE`, `PROCEDURE`, `FUNCTION`, `EVENT`; `CREATE INDEX`, `DROP INDEX` | [`DdlTarget::AlterObject`] |
//! | `DROP [TEMPORARY] TABLE`, `VIEW`, ... (lists) | [`DdlTarget::DropObjects`] |
//! | `RENAME TABLE a TO b, ...`; `ALTER TABLE a RENAME TO b` alone | [`DdlTarget::RenameTables`] |
//! | `TRUNCATE [TABLE]` | [`DdlTarget::Truncate`] |
//!
//! Names are **logical**, as the client wrote them with quoting removed
//! (not lowercased: the catalog applies `names.lower_case_table_names`
//! itself). An unqualified name carries the session's current database
//! as the name layer resolves it, including a `USE` earlier in the same text,
//! and [`DdlName::explicit_db`] says whether the statement qualified it.
//!
//! Only top-level statements get a target. DDL the gateway can't route
//! through the engine (inside `BEGIN NOT ATOMIC ... END`, or in the SQL
//! text of `PREPARE ... FROM '...'` and `EXECUTE IMMEDIATE '...'`) sets
//! [`StatementInfo::nested_ddl`](super::StatementInfo::nested_ddl)
//! instead. Statements in stored program bodies are definitions, not
//! DDL run now, and set neither.
//!
//! [`DdlTarget::to_catalog`] converts a target to `sprig-catalog`'s
//! [`DdlStatement`]. `sprig-sql` (layer 2, execution) may depend on
//! `sprig-catalog` (layer 2, data) under the crate layering, so the
//! conversion lives here and the gateway calls it.

use std::fmt;

use sprig_catalog::ddl::{DdlStatement, ObjectName};
use sprig_catalog::store::ObjectKind as CatalogKind;

use super::NameError;

/// A name in a DDL statement: logical and unquoted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DdlName {
    /// The logical database: as written when the name is qualified, else
    /// the session's current database (`None` when there is none).
    pub db: Option<String>,
    /// Whether the statement qualified the name (`db.name`).
    pub explicit_db: bool,
    /// The object's name.
    pub name: String,
}

impl DdlName {
    /// A qualified name, `db.name`.
    #[must_use]
    pub fn qualified(db: &str, name: &str) -> Self {
        Self {
            db: Some(db.to_owned()),
            explicit_db: true,
            name: name.to_owned(),
        }
    }

    /// An unqualified name, resolved against `current`.
    #[must_use]
    pub fn unqualified(name: &str, current: Option<&str>) -> Self {
        Self {
            db: current.map(str::to_owned),
            explicit_db: false,
            name: name.to_owned(),
        }
    }

    /// The catalog's [`ObjectName`], with the resolved database (so a
    /// `USE` earlier in the same text counts, as MariaDB would see it).
    #[must_use]
    pub fn to_catalog(&self) -> ObjectName {
        ObjectName {
            db: self.db.clone(),
            name: self.name.clone(),
        }
    }
}

impl fmt::Display for DdlName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.db {
            Some(db) if self.explicit_db => write!(f, "{db}.{}", self.name),
            _ => f.write_str(&self.name),
        }
    }
}

/// The kind of object a DDL statement names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DdlKind {
    /// A table.
    Table,
    /// A view.
    View,
    /// A sequence (MariaDB).
    Sequence,
    /// A stored procedure.
    Procedure,
    /// A stored function.
    Function,
    /// A trigger.
    Trigger,
    /// An event.
    Event,
}

impl DdlKind {
    /// The SQL keyword.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            DdlKind::Table => "TABLE",
            DdlKind::View => "VIEW",
            DdlKind::Sequence => "SEQUENCE",
            DdlKind::Procedure => "PROCEDURE",
            DdlKind::Function => "FUNCTION",
            DdlKind::Trigger => "TRIGGER",
            DdlKind::Event => "EVENT",
        }
    }

    /// The catalog's `objects.kind`.
    #[must_use]
    pub const fn to_catalog(self) -> CatalogKind {
        match self {
            DdlKind::Table => CatalogKind::Table,
            DdlKind::View => CatalogKind::View,
            DdlKind::Sequence => CatalogKind::Sequence,
            DdlKind::Procedure => CatalogKind::Procedure,
            DdlKind::Function => CatalogKind::Function,
            DdlKind::Trigger => CatalogKind::Trigger,
            DdlKind::Event => CatalogKind::Event,
        }
    }
}

impl fmt::Display for DdlKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What an [`DdlTarget::AlterObject`] does to its object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterOp {
    /// `ALTER TABLE|VIEW|SEQUENCE|PROCEDURE|FUNCTION|EVENT`. `rename_to`
    /// is set when the statement also renames the object
    /// (`ALTER TABLE t ADD c INT, RENAME TO u`, `ALTER EVENT e RENAME TO
    /// f`); an `ALTER TABLE` that only renames is
    /// [`DdlTarget::RenameTables`] instead.
    Change {
        /// The new name.
        rename_to: Option<DdlName>,
    },
    /// `CREATE [OR REPLACE] [UNIQUE|FULLTEXT|SPATIAL] INDEX [IF NOT EXISTS]
    /// i ON t`.
    CreateIndex {
        /// The index's name.
        name: Option<String>,
        /// `IF NOT EXISTS` (MariaDB).
        if_not_exists: bool,
        /// `OR REPLACE` (MariaDB).
        or_replace: bool,
    },
    /// `DROP INDEX [IF EXISTS] i ON t`.
    DropIndex {
        /// The index's name.
        name: String,
        /// `IF EXISTS` (MariaDB).
        if_exists: bool,
    },
}

/// What a DDL statement does to the catalog, with logical names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DdlTarget {
    /// `CREATE [OR REPLACE] DATABASE|SCHEMA [IF NOT EXISTS] name [options]`.
    CreateDatabase {
        /// The logical name.
        name: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `OR REPLACE` (MariaDB).
        or_replace: bool,
        /// `[DEFAULT] CHARACTER SET [=] x` (or `CHARSET`), lowercased.
        charset: Option<String>,
        /// `[DEFAULT] COLLATE [=] x`, lowercased.
        collation: Option<String>,
        /// Options besides the character set and collation, as their
        /// keywords (`COMMENT`, `ENCRYPTION`): the catalog does not apply
        /// them. `ENCRYPTION = 'N'`, which every MySQL 8 dump writes, is a
        /// no-op and not listed.
        other_options: Vec<String>,
    },
    /// `ALTER DATABASE|SCHEMA [name] options`.
    AlterDatabase {
        /// The logical name: as written, or the current database when the
        /// statement omits it (`None` when there is none either).
        name: Option<String>,
        /// Whether the statement named the database.
        explicit: bool,
        /// `[DEFAULT] CHARACTER SET [=] x` (or `CHARSET`), lowercased.
        charset: Option<String>,
        /// `[DEFAULT] COLLATE [=] x`, lowercased.
        collation: Option<String>,
        /// Options besides the character set and collation (`COMMENT`,
        /// `READ ONLY`, `ENCRYPTION`, `UPGRADE`), as their first keyword.
        other_options: Vec<String>,
    },
    /// `DROP DATABASE|SCHEMA [IF EXISTS] name`.
    DropDatabase {
        /// The logical name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `CREATE [OR REPLACE] [TEMPORARY] TABLE|VIEW|SEQUENCE|PROCEDURE|
    /// FUNCTION|TRIGGER|EVENT [IF NOT EXISTS] name`.
    CreateObject {
        /// The kind.
        kind: DdlKind,
        /// The name.
        name: DdlName,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `OR REPLACE`.
        or_replace: bool,
        /// `TEMPORARY` (tables and MariaDB sequences): session-local, not
        /// catalog DDL. The gateway forwards it without the engine, and the
        /// session pins its backend connection.
        temporary: bool,
        /// A trigger's table (`ON t`).
        parent: Option<DdlName>,
        /// `CREATE TABLE ... LIKE source` (or `(LIKE source)`).
        like: Option<DdlName>,
        /// `CREATE TABLE ... [AS] SELECT ...`: the statement also copies
        /// rows.
        as_select: bool,
    },
    /// `ALTER ...` of an object, and `CREATE INDEX` / `DROP INDEX` as an
    /// alter of their table.
    AlterObject {
        /// The kind.
        kind: DdlKind,
        /// The name.
        name: DdlName,
        /// `ALTER TABLE IF EXISTS`, `ALTER SEQUENCE IF EXISTS` (MariaDB).
        if_exists: bool,
        /// What changes.
        op: AlterOp,
    },
    /// `DROP [TEMPORARY] TABLE|VIEW|SEQUENCE|PROCEDURE|FUNCTION|TRIGGER|
    /// EVENT [IF EXISTS] a, b, ...`.
    DropObjects {
        /// The kind.
        kind: DdlKind,
        /// The names, in order.
        names: Vec<DdlName>,
        /// `IF EXISTS`.
        if_exists: bool,
        /// `DROP TEMPORARY TABLE`: never catalog DDL.
        temporary: bool,
    },
    /// `RENAME TABLE [IF EXISTS] a TO b, c TO d` (across databases too),
    /// and `ALTER TABLE a RENAME TO b` with no other change.
    RenameTables {
        /// The moves, in order.
        pairs: Vec<(DdlName, DdlName)>,
        /// `IF EXISTS` (MariaDB `RENAME TABLE IF EXISTS`, `ALTER TABLE IF
        /// EXISTS`).
        if_exists: bool,
    },
    /// `TRUNCATE [TABLE] name`.
    Truncate {
        /// The table.
        name: DdlName,
    },
}

impl DdlTarget {
    /// Whether the statement is on session-local objects
    /// (`CREATE TEMPORARY TABLE`, `DROP TEMPORARY TABLE`).
    #[must_use]
    pub const fn is_temporary(&self) -> bool {
        matches!(
            self,
            DdlTarget::CreateObject {
                temporary: true,
                ..
            } | DdlTarget::DropObjects {
                temporary: true,
                ..
            }
        )
    }

    /// Whether the statement goes through the catalog's DDL engine; false
    /// for temporary objects, which the gateway forwards (pinning the
    /// session).
    #[must_use]
    pub const fn is_catalog(&self) -> bool {
        !self.is_temporary()
    }

    /// The catalog's [`DdlStatement`]; `None` for statements that are not
    /// catalog DDL ([`DdlTarget::is_catalog`]).
    ///
    /// # Errors
    ///
    /// Fails closed where the engine can't do what the statement asks
    /// (39051 `ER_SPRIG_NAME_REWRITE_UNSUPPORTED`): `CREATE OR REPLACE
    /// DATABASE`, and database options besides the character set and
    /// collation. `IF EXISTS` on `ALTER` and `RENAME TABLE`, and an `ALTER`
    /// that also renames, go to the engine as they are.
    /// `ALTER DATABASE` without a name and no current database is 1046
    /// `ER_NO_DB_ERROR`.
    #[allow(clippy::too_many_lines)] // one arm per statement kind
    pub fn to_catalog(&self) -> Result<Option<DdlStatement>, NameError> {
        if !self.is_catalog() {
            return Ok(None);
        }
        let unsupported = |what: &str| NameError::Unsupported(format!("{what} is not supported"));
        let st = match self {
            DdlTarget::CreateDatabase {
                name,
                if_not_exists,
                or_replace,
                charset,
                collation,
                other_options,
            } => {
                if *or_replace {
                    return Err(unsupported("CREATE OR REPLACE DATABASE"));
                }
                if let Some(o) = other_options.first() {
                    return Err(unsupported(&format!("CREATE DATABASE ... {o}")));
                }
                DdlStatement::CreateDatabase {
                    name: name.clone(),
                    if_not_exists: *if_not_exists,
                    charset: charset.clone(),
                    collation: collation.clone(),
                }
            }
            DdlTarget::AlterDatabase {
                name,
                charset,
                collation,
                other_options,
                ..
            } => {
                if let Some(o) = other_options.first() {
                    return Err(unsupported(&format!("ALTER DATABASE ... {o}")));
                }
                let Some(name) = name else {
                    return Err(NameError::Refused {
                        errno: 1046,
                        sqlstate: *b"3D000",
                        message: "No database selected".into(),
                    });
                };
                DdlStatement::AlterDatabase {
                    name: name.clone(),
                    charset: charset.clone(),
                    collation: collation.clone(),
                }
            }
            DdlTarget::DropDatabase { name, if_exists } => DdlStatement::DropDatabase {
                name: name.clone(),
                if_exists: *if_exists,
            },
            DdlTarget::CreateObject {
                kind,
                name,
                if_not_exists,
                or_replace,
                parent,
                ..
            } => DdlStatement::CreateObject {
                kind: kind.to_catalog(),
                name: name.to_catalog(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
                parent: parent.as_ref().map(DdlName::to_catalog),
            },
            DdlTarget::AlterObject {
                kind,
                name,
                if_exists,
                op,
            } => DdlStatement::AlterObject {
                kind: kind.to_catalog(),
                name: name.to_catalog(),
                if_exists: *if_exists,
                rename_to: match op {
                    AlterOp::Change {
                        rename_to: Some(to),
                    } => Some(to.to_catalog()),
                    _ => None,
                },
            },
            DdlTarget::DropObjects {
                kind,
                names,
                if_exists,
                ..
            } => DdlStatement::DropObjects {
                kind: kind.to_catalog(),
                names: names.iter().map(DdlName::to_catalog).collect(),
                if_exists: *if_exists,
            },
            DdlTarget::RenameTables { pairs, if_exists } => DdlStatement::RenameTables {
                pairs: pairs
                    .iter()
                    .map(|(a, b)| (a.to_catalog(), b.to_catalog()))
                    .collect(),
                if_exists: *if_exists,
            },
            DdlTarget::Truncate { name } => DdlStatement::Truncate {
                name: name.to_catalog(),
            },
        };
        Ok(Some(st))
    }
}

#[cfg(test)]
mod tests;
