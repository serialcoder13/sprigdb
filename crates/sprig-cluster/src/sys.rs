//! The internal schema `__sprig_sys` and the cluster's own tables in it.
//! The mover owns three more.

use sprig_common::names::{SYS_SCHEMA, internal_object, quote};

use crate::sql::{ProbeError, ProbeSession};

/// `__sprig_sys.__sprig_probe`: XA round trips and other probe writes.
#[must_use]
pub fn probe_table() -> String {
    format!("{}.{}", quote(SYS_SCHEMA), quote(&internal_object("probe")))
}

/// `__sprig_sys.__sprig_heartbeat`: one row per writer node.
#[must_use]
pub fn heartbeat_table() -> String {
    format!(
        "{}.{}",
        quote(SYS_SCHEMA),
        quote(&internal_object("heartbeat"))
    )
}

/// A scratch object of a feature probe: `__sprig_sys.__sprig_probe_<suffix>`.
#[must_use]
pub fn scratch(suffix: &str) -> String {
    format!(
        "{}.{}",
        quote(SYS_SCHEMA),
        quote(&internal_object(&format!("probe_{suffix}")))
    )
}

/// The statements that create the schema and the cluster's own tables.
/// Each is idempotent.
#[must_use]
pub fn ddl() -> [String; 3] {
    [
        format!(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            quote(SYS_SCHEMA)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT UNSIGNED NOT NULL PRIMARY KEY, \
             v BIGINT UNSIGNED NOT NULL, written_at DATETIME(6) NOT NULL) ENGINE=InnoDB",
            probe_table()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (writer_node VARCHAR(64) NOT NULL PRIMARY KEY, \
             seq BIGINT UNSIGNED NOT NULL, written_at DATETIME(6) NOT NULL) ENGINE=InnoDB",
            heartbeat_table()
        ),
    ]
}

/// Creates `__sprig_sys` and its tables if missing. Run on a writer: the
/// DDL replicates (or runs in TOI on Galera).
///
/// # Errors
/// A statement failed, typically for a missing privilege on `__sprig_sys`.
pub async fn ensure(session: &mut ProbeSession) -> Result<(), ProbeError> {
    for stmt in ddl() {
        session.exec(&stmt).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_come_from_sprig_common() {
        assert_eq!(probe_table(), "`__sprig_sys`.`__sprig_probe`");
        assert_eq!(heartbeat_table(), "`__sprig_sys`.`__sprig_heartbeat`");
        assert_eq!(scratch("f"), "`__sprig_sys`.`__sprig_probe_f`");
        assert!(ddl()[0].contains("utf8mb4_bin"));
    }
}
