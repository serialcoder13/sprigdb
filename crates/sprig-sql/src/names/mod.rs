//! Name virtualization (chapter 07).
//!
//! - [`map`]: the physical-name grammar (from [`sprig_common::names`]) and
//!   the per-tenant [`NameMap`] (build step 1; the catalog-backed map comes
//!   with chapter 08 step 4).
//! - [`rewrite`]: the statement rewrite: lexing per persona, every name
//!   position, span editing (build step 2) and the substitutions for
//!   `DATABASE()`, account functions and query attributes (build step 3).
//!
//! Not here yet: the `SHOW` family (step 4), `information_schema` inline
//! views (step 5), `performance_schema`, `sys` and `mysql` (step 6),
//! column-definition and message mapping (step 7), hiding beyond the
//! reserved-name checks the rewrite makes (step 8) and `DEFINER` (step 9).

pub mod map;
pub mod rewrite;

pub use map::{LogicalDb, MemoryNameMap, NameMap, Purpose, SchemaSet};
pub use rewrite::{
    AccountInfo, Edit, EditKind, Engine, RewriteCtx, Rewritten, StatementInfo, StmtKind, SystemRef,
    SystemSchema, UseTarget, rewrite, unrewrite,
};

use std::fmt;

use sprig_common::SprigError;

/// What kind of object a name denotes, for error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// A table (or anything in a table position: views, sequences, CTEs).
    Table,
    /// A view, where the statement says so.
    View,
    /// A stored procedure.
    Procedure,
    /// A stored function.
    Function,
    /// A trigger.
    Trigger,
    /// An event.
    Event,
    /// A sequence (MariaDB).
    Sequence,
    /// An index.
    Index,
    /// A column.
    Column,
    /// A database.
    Database,
}

impl ObjectKind {
    /// The word MySQL uses in messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Table => "TABLE",
            ObjectKind::View => "VIEW",
            ObjectKind::Procedure => "PROCEDURE",
            ObjectKind::Function => "FUNCTION",
            ObjectKind::Trigger => "TRIGGER",
            ObjectKind::Event => "EVENT",
            ObjectKind::Sequence => "SEQUENCE",
            ObjectKind::Index => "INDEX",
            ObjectKind::Column => "COLUMN",
            ObjectKind::Database => "DATABASE",
        }
    }
}

impl fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a statement's names could not be rewritten. Every message holds
/// logical names only.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    /// 1049 `ER_BAD_DB_ERROR`: the tenant has no such database. The caller
    /// forces one catalog refresh and retries first.
    #[error("Unknown database '{0}'")]
    UnknownDatabase(String),
    /// A reference to a reserved (`__sprig_`) object, which fails as if the
    /// object were missing: 1146, 1305, 1360 or 1539 by kind.
    #[error("{}", missing_message(*.kind, .db.as_deref(), .name))]
    Missing {
        /// What the statement looked for.
        kind: ObjectKind,
        /// The logical database, if known.
        db: Option<String>,
        /// The object's name.
        name: String,
    },
    /// 1054 `ER_BAD_FIELD_ERROR`: a reference to a reserved (`_sprig_`)
    /// column.
    #[error("Unknown column '{0}' in 'field list'")]
    UnknownColumn(String),
    /// 39050 `ER_SPRIG_RESERVED_NAME`: creating something with a reserved
    /// name.
    #[error("'{0}' is a reserved name")]
    Reserved(String),
    /// 39051 `ER_SPRIG_NAME_REWRITE_UNSUPPORTED`: a name position the
    /// rewriter does not handle. Never forwarded unrewritten.
    #[error("name rewrite unsupported: {0}")]
    Unsupported(String),
}

fn missing_message(kind: ObjectKind, db: Option<&str>, name: &str) -> String {
    let qualified = match db {
        Some(db) => format!("{db}.{name}"),
        None => name.to_owned(),
    };
    match kind {
        ObjectKind::Procedure | ObjectKind::Function => {
            format!("{} {qualified} does not exist", kind.as_str())
        }
        ObjectKind::Trigger => "Trigger does not exist".to_owned(),
        ObjectKind::Event => format!("Unknown event '{name}'"),
        ObjectKind::Column => format!("Unknown column '{name}' in 'field list'"),
        ObjectKind::Database => format!("Unknown database '{name}'"),
        ObjectKind::Index => format!("Key '{name}' doesn't exist in table"),
        ObjectKind::Table | ObjectKind::View | ObjectKind::Sequence => {
            format!("Table '{qualified}' doesn't exist")
        }
    }
}

impl NameError {
    /// The MySQL error number.
    #[must_use]
    pub const fn errno(&self) -> u16 {
        match self {
            NameError::UnknownDatabase(_) => 1049,
            NameError::Missing { kind, .. } => match kind {
                ObjectKind::Procedure | ObjectKind::Function => 1305,
                ObjectKind::Trigger => 1360,
                ObjectKind::Event => 1539,
                ObjectKind::Column => 1054,
                ObjectKind::Database => 1049,
                ObjectKind::Index => 1176,
                ObjectKind::Table | ObjectKind::View | ObjectKind::Sequence => 1146,
            },
            NameError::UnknownColumn(_) => 1054,
            NameError::Reserved(_) => 39050,
            NameError::Unsupported(_) => 39051,
        }
    }

    /// The SQLSTATE.
    #[must_use]
    pub const fn sqlstate(&self) -> [u8; 5] {
        match self {
            NameError::UnknownDatabase(_) | NameError::Reserved(_) => *b"42000",
            NameError::Missing { kind, .. } => match kind {
                ObjectKind::Procedure | ObjectKind::Function | ObjectKind::Database => *b"42000",
                ObjectKind::Trigger | ObjectKind::Event => *b"HY000",
                ObjectKind::Column => *b"42S22",
                ObjectKind::Index => *b"42000",
                ObjectKind::Table | ObjectKind::View | ObjectKind::Sequence => *b"42S02",
            },
            NameError::UnknownColumn(_) => *b"42S22",
            NameError::Unsupported(_) => *b"HY000",
        }
    }

    /// Whether the caller should force one catalog refresh and retry before
    /// returning the error (an unknown database may be a stale cache).
    #[must_use]
    pub const fn refresh_and_retry(&self) -> bool {
        matches!(self, NameError::UnknownDatabase(_))
    }

    /// The error as a [`SprigError`].
    ///
    /// 39050 and 39051 are built as standard errors with their numbers and
    /// SQLSTATEs until `sprig_common::SprigCode` gains `ReservedName` and
    /// `NameRewriteUnsupported` (Appendix E).
    #[must_use]
    pub fn into_sprig(self) -> SprigError {
        let (errno, state) = (self.errno(), self.sqlstate());
        SprigError::standard(errno, state, self.to_string())
    }
}

impl From<NameError> for SprigError {
    fn from(e: NameError) -> Self {
        e.into_sprig()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_carry_mysql_numbers_and_logical_text() {
        let e = NameError::UnknownDatabase("billing".into());
        assert_eq!((e.errno(), &e.sqlstate()), (1049, b"42000"));
        assert_eq!(e.to_string(), "Unknown database 'billing'");
        assert!(e.refresh_and_retry());
        let e = NameError::Missing {
            kind: ObjectKind::Table,
            db: Some("billing".into()),
            name: "__sprig_dtx".into(),
        };
        assert_eq!(e.errno(), 1146);
        assert_eq!(e.to_string(), "Table 'billing.__sprig_dtx' doesn't exist");
        let e = NameError::Missing {
            kind: ObjectKind::Procedure,
            db: Some("billing".into()),
            name: "__sprig_p".into(),
        };
        assert_eq!(e.to_string(), "PROCEDURE billing.__sprig_p does not exist");
        assert_eq!(NameError::Reserved("t1_x".into()).errno(), 39050);
        let s = NameError::Unsupported("x".into()).into_sprig();
        assert_eq!(s.protocol().mysql_errno, 39051);
        assert_eq!(&s.protocol().sqlstate, b"HY000");
    }
}
