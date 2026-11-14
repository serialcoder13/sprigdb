//! Result metadata on port 3306.
//!
//! The gateway's backend connections for MySQL persona sessions ask
//! MariaDB for extended metadata (`MARIADB_CLIENT_EXTENDED_METADATA`), which
//! marks JSON values with the format `json`. [`mysql_column`] turns such a
//! column into MySQL's `JSON` type, 245, shaped as MySQL 8.4 sends it
//! (captured on 8 November 2026):
//!
//! | | MariaDB 11.8 | MySQL 8.4 |
//! |---|---|---|
//! | `JSON` column | `BLOB` (252), client charset, `BLOB BINARY` | 245, charset 63 (`binary`), length 4294967295, `BLOB BINARY` |
//! | `JSON_OBJECT(..)` | `VAR_STRING` (253), client charset | 245, client charset, length 4294967292, `BINARY` |
//!
//! It also gives "not fixed" decimals MySQL's value: MariaDB sends 39 where
//! MySQL sends 31. The extended block itself is dropped, since a MySQL
//! client never negotiates it.

use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};

/// MariaDB's `NOT_FIXED_DEC`.
const MARIADB_NOT_FIXED_DEC: u8 = 39;
/// MySQL's `NOT_FIXED_DEC`.
const MYSQL_NOT_FIXED_DEC: u8 = 31;

/// Whether MariaDB's extended metadata marks the column as JSON.
#[must_use]
pub fn is_json(c: &ColumnDef) -> bool {
    c.ext_format_name()
        .or_else(|| c.ext_type_name())
        .is_some_and(|f| f.eq_ignore_ascii_case(b"json"))
}

/// Reshapes one column definition for a MySQL 8.4 client (see the module
/// documentation). Returns whether anything changed.
pub fn mysql_column(c: &mut ColumnDef) -> bool {
    let mut changed = false;
    if is_json(c) {
        c.col_type = column_type::JSON;
        if c.org_table.is_empty() {
            // An expression: the result charset stays.
            c.length = 4_294_967_292;
            c.flags = (c.flags & !ColumnFlags::BLOB.bits()) | ColumnFlags::BINARY.bits();
            c.decimals = MYSQL_NOT_FIXED_DEC;
        } else {
            c.charset = 63;
            c.length = u32::MAX;
            c.flags |= ColumnFlags::BLOB.bits() | ColumnFlags::BINARY.bits();
        }
        changed = true;
    }
    if c.decimals == MARIADB_NOT_FIXED_DEC {
        c.decimals = MYSQL_NOT_FIXED_DEC;
        changed = true;
    }
    if c.ext_type.take().is_some() {
        changed = true;
    }
    changed
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn col(table: &str, ty: u8, ext: Option<&[u8]>) -> ColumnDef {
        ColumnDef {
            org_table: Bytes::copy_from_slice(table.as_bytes()),
            col_type: ty,
            charset: 224,
            length: 4_294_967_295,
            flags: 144,
            ext_type: ColumnDef::ext_metadata(None, ext),
            ..ColumnDef::default()
        }
    }

    #[test]
    fn json_columns_and_expressions_become_type_245() {
        let mut c = col("j", column_type::BLOB, Some(b"json"));
        assert!(mysql_column(&mut c));
        assert_eq!(
            (c.col_type, c.charset, c.length, c.flags),
            (245, 63, u32::MAX, 144)
        );
        assert!(c.ext_type.is_none());
        let mut e = col("", column_type::VAR_STRING, Some(b"json"));
        e.flags = 0;
        e.decimals = 39;
        assert!(mysql_column(&mut e));
        assert_eq!(
            (e.col_type, e.charset, e.length, e.flags, e.decimals),
            (245, 224, 4_294_967_292, 128, 31)
        );
    }

    #[test]
    fn other_columns_keep_their_type() {
        let mut t = col("t", column_type::BLOB, None);
        assert!(!mysql_column(&mut t));
        assert_eq!(t.col_type, column_type::BLOB);
        let mut u = col("t", column_type::STRING, None);
        u.ext_type = ColumnDef::ext_metadata(Some(b"uuid"), None);
        assert!(mysql_column(&mut u));
        assert_eq!(u.col_type, column_type::STRING);
        assert!(u.ext_type.is_none());
    }
}
