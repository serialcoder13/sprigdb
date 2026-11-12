//! Row-level security for paths that stream a table's rows out of the
//! gateway's session rewrite (chapter 29, "Change feed consumers"; RLS
//! round 4, decisions RL36 onward).
//!
//! ```text
//! change topic t.changes read by a principal (SQL, Kafka, a stream trigger,
//! a sink FROM STREAM t.changes):
//!   SELECT on the source table t itself (not only on the name "t.changes")
//!   policies on t bind the principal → 39366 (the topic's records are the
//!     table's rows; they aren't filtered per record)
//! ```
//!
//! The checks are the same at creation (a trigger's or sink's) and at each
//! run, so a policy, role or grant change after creation stops delivery.
//!
//! **Per-record filtering (RLS round 5, decision RL44).** A session the
//! table's policies bind reads the topic through [`record_filter`]: its
//! `SELECT` policies, rendered with its own session values exactly as the
//! rewrite renders them for the table, are evaluated by MariaDB over each
//! record's `before` and `after` images (`JSON_TABLE` with the table's
//! column types and collations, [`filter_columns`]):
//!
//! ```text
//! c: after visible        d: before visible       ddl, t: always
//! u: both → u; before only → d (after null); after only → c (before null)
//! anything else is left out (an offset gap; nothing is renumbered)
//! ```
//!
//! A policy that reads a binary, bit, spatial or vector column (the record
//! carries it as base64) hides every image of the table.

use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_streams::model::{StreamDef, StreamKind};

/// Whether `grants` may read change topic `def`'s records: `Ok` for an
/// event stream, or a change topic whose source table the grants may
/// `SELECT` and whose policies don't bind them. 1142 without `SELECT` on
/// the table, 39366 when its policies bind.
///
/// # Errors
/// As above.
pub fn change_topic_readable(
    grants: &dyn SessionGrants,
    cat: &TenantCatalog,
    def: &StreamDef,
    lctn: bool,
    user: &str,
) -> Result<()> {
    if def.kind != StreamKind::Changes {
        return Ok(());
    }
    if grants.unrestricted() {
        return Ok(());
    }
    let table = def.source_name.clone().unwrap_or_default();
    let (Some(object), Some(db)) = (def.source_object, cat.databases.get(def.db)) else {
        // A change topic without its table: nothing to read it by.
        return Err(denied(user, "?", &table));
    };
    let db_key = database_key(&db.name, lctn);
    let on = Target::Object {
        db_key: &db_key,
        db: def.db,
        object,
    };
    if !grants.allows(Action::Select, on) {
        return Err(denied(user, &db.name, &table));
    }
    if grants.row_filter(on, Action::Select).is_some() {
        return Err(bound(&table, &def.name));
    }
    Ok(())
}

/// How a principal may read a change topic (RL44).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicAccess {
    /// Every record as stored.
    All,
    /// The records the table's policies show it ([`record_filter`]).
    Filtered,
}

/// Whether `grants` may read change topic `def`, and how: `SELECT` on its
/// source table (1142 without), its records filtered when the table's
/// policies bind the grants (RL44; [`change_topic_readable`] refused
/// those).
///
/// # Errors
/// 1142.
pub fn change_topic_access(
    grants: &dyn SessionGrants,
    cat: &TenantCatalog,
    def: &StreamDef,
    lctn: bool,
    user: &str,
) -> Result<TopicAccess> {
    match change_topic_readable(grants, cat, def, lctn, user) {
        Ok(()) => Ok(TopicAccess::All),
        Err(e) if e.code() == Some(SprigCode::RlsUnsupported) => Ok(TopicAccess::Filtered),
        Err(e) => Err(e),
    }
}

/// 39366 for change topic `topic` of table `table`.
#[must_use]
pub fn bound(table: &str, topic: &str) -> SprigError {
    SprigError::new(
        SprigCode::RlsUnsupported,
        format!(
            "ER_SPRIG_RLS_UNSUPPORTED: row-level security on '{table}' binds this user, and \
             change topic '{topic}' carries the table's rows unfiltered"
        ),
    )
}

fn denied(user: &str, db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("SELECT command denied to user '{user}' for table `{db}`.`{table}`"),
    )
}

/// A column of a protected table as a change record's image types it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterColumn {
    /// The name.
    pub name: String,
    /// Its `JSON_TABLE` type (with character set and collation).
    pub ty: String,
    /// The record carries it in another form (base64, a JSON array): a
    /// policy reading it can't be evaluated on the image.
    pub opaque: bool,
    /// A binary string column's own type, for images that carry its bytes
    /// as a plain JSON string (keyspace notifications' keys).
    pub plain: Option<String>,
}

/// The `JSON_TABLE` type of a column from `information_schema.COLUMNS`
/// (`DATA_TYPE`, `COLUMN_TYPE`, `CHARACTER_SET_NAME`, `COLLATION_NAME`),
/// and whether it is opaque ([`FilterColumn::opaque`]).
#[must_use]
pub fn json_table_type(
    data_type: &str,
    column_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> (String, bool) {
    let dt = data_type.to_ascii_lowercase();
    let text_of = |base: &str| match (charset, collation) {
        (Some(cs), Some(co)) => format!("{base} CHARACTER SET {cs} COLLATE {co}"),
        (Some(cs), None) => format!("{base} CHARACTER SET {cs}"),
        _ => base.to_owned(),
    };
    match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
        | "numeric" | "float" | "double" | "real" | "date" | "datetime" | "timestamp" | "time"
        | "year" => (
            column_type.to_ascii_lowercase().replace(" zerofill", ""),
            false,
        ),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
            (text_of(&column_type.to_ascii_lowercase()), false)
        }
        // JSON_TABLE takes no ENUM or SET: their text, compared as text.
        "enum" | "set" => (text_of("text"), false),
        "uuid" | "inet4" | "inet6" => ("varchar(64) character set ascii".to_owned(), false),
        _ => ("longtext".to_owned(), true),
    }
}

/// An `information_schema.COLUMNS` row as [`filter_columns`] reads it.
type ColumnRow = (String, String, String, Option<String>, Option<String>);

/// Columns by (tenant, table, schema version).
type ColumnCache =
    std::sync::Mutex<std::collections::HashMap<(u64, u64, u64), std::sync::Arc<Vec<FilterColumn>>>>;

fn column_cache() -> &'static ColumnCache {
    static C: std::sync::OnceLock<ColumnCache> = std::sync::OnceLock::new();
    C.get_or_init(Default::default)
}

/// Table `table`'s columns in shard schema `schema` of `cluster` as
/// [`FilterColumn`]s, cached by the table's schema version. Internal
/// columns (`_sprig…`, `__sprig…`) are left out: records never carry them.
///
/// # Errors
/// The cluster's.
pub async fn filter_columns(
    svc: &super::TenantServices,
    tenant: u64,
    object: &sprig_catalog::store::ObjectRow,
    cluster: &sprig_common::ids::ClusterId,
    schema: &str,
) -> Result<std::sync::Arc<Vec<FilterColumn>>> {
    use mysql_async::prelude::Queryable;
    let key = (tenant, object.object_id.0, object.schema_version);
    if let Some(c) = column_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
    {
        return Ok(std::sync::Arc::clone(c));
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let rows: Vec<ColumnRow> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (schema, object.name.as_str()),
        )
        .await
        .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}")))?;
    drop(conn);
    if rows.is_empty() {
        return Err(SprigError::internal(format!(
            "table {} has no columns in {schema}",
            object.name
        )));
    }
    let cols: Vec<FilterColumn> = rows
        .into_iter()
        .filter(|(n, ..)| !n.starts_with("_sprig") && !n.starts_with("__sprig"))
        .map(|(name, dt, ct, cs, co)| {
            let (ty, opaque) = json_table_type(&dt, &ct, cs.as_deref(), co.as_deref());
            // A name a JSON path can't spell plainly is never read.
            let odd = name.contains(['"', '\\', '\'']);
            let binary = matches!(
                dt.to_ascii_lowercase().as_str(),
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            );
            FilterColumn {
                name,
                ty,
                opaque: opaque || odd,
                plain: (binary && !odd).then(|| ct.to_ascii_lowercase()),
            }
        })
        .collect();
    let cols = std::sync::Arc::new(cols);
    column_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key, std::sync::Arc::clone(&cols));
    Ok(cols)
}

/// The columns a predicate rendered over `qual` reads (lower case).
fn columns_read(rendered: &str, qual: &str) -> Vec<String> {
    let q = format!("{qual}.");
    let mut out = Vec::new();
    let mut rest = rendered;
    while let Some(at) = rest.find(&q) {
        let after = &rest[at + q.len()..];
        let col = if let Some(r) = after.strip_prefix('`') {
            r.split('`').next().unwrap_or_default().to_owned()
        } else {
            after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
                .collect()
        };
        out.push(col.to_lowercase());
        rest = after;
    }
    out
}

/// The `JSON_TABLE` column list of `columns`.
#[must_use]
pub fn json_table_columns(columns: &[FilterColumn]) -> String {
    columns
        .iter()
        .map(|c| {
            let path = if c.opaque {
                "$.__sprig_none".to_owned()
            } else {
                format!("$.\"{}\"", c.name)
            };
            format!(
                "{} {} PATH {}",
                sprig_sql::rls::quote(&c.name),
                c.ty,
                sprig_sql::rls::sql_string(&path, false)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How session `ts` reads change topic `def` of table `object` (database
/// `db_name`): `None` when the table's policies don't bind it, else its
/// `SELECT` policies rendered with its session values over
/// [`sprig_streams::read::FILTER_QUAL`] (decision RL44).
///
/// # Errors
/// The rewrite's (39364, 39366 for a subtree above
/// `rls.inline_subtree_max`).
#[allow(clippy::too_many_arguments)]
pub async fn record_filter(
    ts: &super::session::TenantSession,
    cat: &TenantCatalog,
    db_name: &str,
    object: sprig_common::ids::ObjectId,
    columns: &[FilterColumn],
    lex: sprig_sql::lex::LexOptions,
    current: Option<&str>,
) -> Result<Option<sprig_streams::read::RecordFilter>> {
    use sprig_sql::rls::Resolver as _;
    let grants = super::authz::privileges(ts, cat);
    if grants.unrestricted() {
        return Ok(None);
    }
    let Some(rls) = grants.row_policies() else {
        return Ok(None);
    };
    if !rls.tables.contains_key(&object) {
        return Ok(None);
    }
    let Some(obj) = cat.objects.get(object) else {
        return Err(SprigError::internal("a change topic's table is gone"));
    };
    let mut values = super::rls::SessionValues::of(ts, cat);
    super::rls::bind_tree(ts, cat, &rls, &mut values).await?;
    let lctn = ts.lower_case_table_names();
    let tainted = std::collections::HashSet::new();
    let temp = |_: Option<&str>, _: &str| false;
    let qual = sprig_streams::read::FILTER_QUAL;
    let predicate = {
        let r = super::rls::Resolve::new(cat, &rls, current, lctn, &temp, &values, lex, &tainted);
        let key = r
            .protected(Some(db_name), &obj.name)
            .ok_or_else(|| SprigError::internal("a protected table the resolver can't find"))?;
        r.predicate(key, &[sprig_sql::rls::Cmd::Select], qual)?
    };
    let (columns, predicate) = decode_binary(columns, predicate, qual);
    let opaque: Vec<String> = columns
        .iter()
        .filter(|c| c.opaque)
        .map(|c| c.name.to_lowercase())
        .collect();
    let predicate = if columns_read(&predicate, qual)
        .iter()
        .any(|c| opaque.contains(c))
    {
        "FALSE".to_owned()
    } else {
        predicate
    };
    Ok(Some(sprig_streams::read::RecordFilter {
        columns: json_table_columns(&columns),
        predicate,
    }))
}

/// Binary string columns (`BINARY`, `VARBINARY`, the `BLOB`s) that
/// `predicate` reads, decoded from the image's base64 rather than hiding
/// the record (RLS round 6, decision RL56: a key-value namespace's key
/// `k` is `VARBINARY`, so key policies would otherwise hide every change
/// of a namespace): each is read from the image as text and every
/// reference `qual.c` in the predicate becomes `FROM_BASE64(qual.c)`.
/// Bit, spatial and vector columns stay opaque.
fn decode_binary(
    columns: &[FilterColumn],
    mut predicate: String,
    qual: &str,
) -> (Vec<FilterColumn>, String) {
    let read = columns_read(&predicate, qual);
    let mut out = columns.to_vec();
    for c in &mut out {
        if c.opaque && c.plain.is_some() && read.contains(&c.name.to_lowercase()) {
            c.opaque = false;
            c.ty = "longtext CHARACTER SET ascii".to_owned();
            predicate = map_column(&predicate, qual, &c.name, |r| format!("FROM_BASE64({r})"));
        }
    }
    (out, predicate)
}

/// `rendered` with each reference `qual.name` (bare or quoted, any case)
/// replaced by `with(reference)`.
fn map_column(rendered: &str, qual: &str, name: &str, with: impl Fn(&str) -> String) -> String {
    let q = format!("{qual}.");
    let mut out = String::with_capacity(rendered.len() + 16);
    let mut rest = rendered;
    while let Some(at) = rest.find(&q) {
        let after = &rest[at + q.len()..];
        let (col, len) = if let Some(r) = after.strip_prefix('`') {
            let c = r.split('`').next().unwrap_or_default();
            (c.to_owned(), c.len() + 2)
        } else {
            let c: String = after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
                .collect();
            let n = c.len();
            (c, n)
        };
        out.push_str(&rest[..at]);
        let reference = &rest[at..at + q.len() + len];
        if col.eq_ignore_ascii_case(name) {
            out.push_str(&with(reference));
        } else {
            out.push_str(reference);
        }
        rest = &rest[at + q.len() + len..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_types() {
        assert_eq!(
            json_table_type(
                "varchar",
                "varchar(64)",
                Some("utf8mb4"),
                Some("utf8mb4_general_ci")
            ),
            (
                "varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci".to_owned(),
                false
            )
        );
        assert_eq!(
            json_table_type("int", "int(10) unsigned zerofill", None, None),
            ("int(10) unsigned".to_owned(), false)
        );
        assert_eq!(
            json_table_type(
                "enum",
                "enum('a','b')",
                Some("utf8mb4"),
                Some("utf8mb4_bin")
            )
            .0,
            "text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );
        assert!(json_table_type("varbinary", "varbinary(16)", None, None).1);
        assert!(json_table_type("point", "point", None, None).1);
    }

    #[test]
    fn columns_a_predicate_reads() {
        let r = "(`__sprig_ci`.`owner` = 'a') AND (`__sprig_ci`.Blob_C IS NULL)";
        assert_eq!(columns_read(r, "`__sprig_ci`"), vec!["owner", "blob_c"]);
        let cols = vec![
            FilterColumn {
                name: "owner".into(),
                ty: "varchar(64)".into(),
                opaque: false,
                plain: None,
            },
            FilterColumn {
                name: "b".into(),
                ty: "longtext".into(),
                opaque: true,
                plain: Some("varbinary(16)".into()),
            },
        ];
        assert_eq!(
            json_table_columns(&cols),
            "`owner` varchar(64) PATH '$.\"owner\"', `b` longtext PATH '$.__sprig_none'"
        );
        // RL56: a binary column the predicate reads is decoded from base64.
        let (c2, p2) = decode_binary(
            &cols,
            "(`__sprig_ci`.b LIKE 'a:%') AND (`__sprig_ci`.`B` <> `__sprig_ci`.bx)".to_owned(),
            "`__sprig_ci`",
        );
        assert_eq!(
            p2,
            "(FROM_BASE64(`__sprig_ci`.b) LIKE 'a:%') AND (FROM_BASE64(`__sprig_ci`.`B`) <> `__sprig_ci`.bx)"
        );
        assert_eq!(
            json_table_columns(&c2),
            "`owner` varchar(64) PATH '$.\"owner\"', `b` longtext CHARACTER SET ascii PATH '$.\"b\"'"
        );
        // Not read: stays opaque.
        let (c3, _) = decode_binary(&cols, "`__sprig_ci`.owner = 'x'".to_owned(), "`__sprig_ci`");
        assert!(c3[1].opaque);
    }
}
