//! `information_schema` inline views (chapter 07, section `information_schema`;
//! build step 5).
//!
//! Each reference to an `information_schema` table that names schemas is
//! replaced by a derived table over the backend's view, restricted to the
//! tenant's representative schemas (`Purpose::Metadata`), with logical
//! names in the select list and internal objects hidden. The rest of the
//! query is untouched, so its semantics stay exact.
//!
//! ```text
//! SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'billing'
//!   ─► SELECT TABLE_NAME FROM (
//!        SELECT `TABLE_CATALOG`, 'billing' AS `TABLE_SCHEMA`, `TABLE_NAME`, ...
//!        FROM information_schema.TABLES
//!        WHERE `TABLE_SCHEMA` = 't42_d1_g1_s1' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_'
//!      ) AS `TABLES` WHERE TABLE_SCHEMA = 'billing'
//! ```
//!
//! - **One equality per schema.** A tenant with several schemas gets one
//!   `UNION ALL` branch per schema, each with `<restriction column> =
//!   '<physical>'`: MariaDB opens only that schema, where an `IN` list or
//!   `LIKE` scans every schema on the server
//!   (`decisions/spike-schemas-per-cluster.md`). The restriction column is
//!   the one MariaDB's lookup uses (verified with `EXPLAIN`'s `Scanned 1
//!   database`): `EVENT_OBJECT_SCHEMA` for `TRIGGERS`, `CONSTRAINT_SCHEMA`
//!   for `REFERENTIAL_CONSTRAINTS` and `CHECK_CONSTRAINTS`, `TABLE_SCHEMA`
//!   elsewhere.
//! - **Pushdown.** An outer `<schema column> = 'x'` or `IN ('x', ...)`
//!   conjunct picks the branches; equalities on the table-name column are
//!   copied into each branch. The outer predicate stays.
//! - **Hiding.** `LEFT(name, n) <> 'prefix'`, which works in every
//!   `sql_mode`; indexes and constraints on hidden columns by a `NOT IN`
//!   over the same schema.
//! - **Local tables.** `SCHEMATA` (the persona's system schemas as literal
//!   rows plus one branch per database) and `PROCESSLIST` (the gateway's
//!   sessions as literal rows).
//! - **Pass-through** for tables without schema names; **1142** for
//!   server-wide tables (`INNODB_*`, `GLOBAL_STATUS`, ...); **39051** for
//!   the `SPRIG_` views (build step 11) and for mapped tables inside a
//!   view or stored program, which would bake physical names into it.

use std::collections::HashMap;
use std::fmt::Write as _;

use sprig_common::names::{GIPK_COLUMN, quote};
use sprig_proto_mysql::packets::Persona;

use super::NameError;
use super::hide::Visibility;
use super::map::{LogicalDb, NameMap, Purpose, SchemaName};
use super::rewrite::{StmtKind, sql_string};
use super::show::{ProcessRow, SessionSource, database_names};
use crate::compat::schema::{self, Src};
use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};

/// The column lists of `information_schema` tables, in order, per backend
/// version (chapter 07, rule 2), so `SELECT *` keeps its shape.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRegistry {
    tables: HashMap<String, Vec<String>>,
}

/// MariaDB 11.8's columns of the tables this module rewrites, captured
/// from the Compose node.
const MARIADB_11_8: &[(&str, &str)] = &[
    (
        "CHECK_CONSTRAINTS",
        "CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,LEVEL,CHECK_CLAUSE",
    ),
    (
        "COLUMNS",
        "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,PRIVILEGES,COLUMN_COMMENT,IS_GENERATED,GENERATION_EXPRESSION,IS_SYSTEM_TIME_PERIOD_START,IS_SYSTEM_TIME_PERIOD_END",
    ),
    (
        "COLUMN_PRIVILEGES",
        "GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,PRIVILEGE_TYPE,IS_GRANTABLE",
    ),
    (
        "EVENTS",
        "EVENT_CATALOG,EVENT_SCHEMA,EVENT_NAME,DEFINER,TIME_ZONE,EVENT_BODY,EVENT_DEFINITION,EVENT_TYPE,EXECUTE_AT,INTERVAL_VALUE,INTERVAL_FIELD,SQL_MODE,STARTS,ENDS,STATUS,ON_COMPLETION,CREATED,LAST_ALTERED,LAST_EXECUTED,EVENT_COMMENT,ORIGINATOR,CHARACTER_SET_CLIENT,COLLATION_CONNECTION,DATABASE_COLLATION",
    ),
    (
        "KEY_COLUMN_USAGE",
        "CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,POSITION_IN_UNIQUE_CONSTRAINT,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME",
    ),
    (
        "PARAMETERS",
        "SPECIFIC_CATALOG,SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,DTD_IDENTIFIER,ROUTINE_TYPE",
    ),
    (
        "PARTITIONS",
        "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,SUBPARTITION_NAME,PARTITION_ORDINAL_POSITION,SUBPARTITION_ORDINAL_POSITION,PARTITION_METHOD,SUBPARTITION_METHOD,PARTITION_EXPRESSION,SUBPARTITION_EXPRESSION,PARTITION_DESCRIPTION,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,CREATE_TIME,UPDATE_TIME,CHECK_TIME,CHECKSUM,PARTITION_COMMENT,NODEGROUP,TABLESPACE_NAME",
    ),
    (
        "PROCESSLIST",
        "ID,USER,HOST,DB,COMMAND,TIME,STATE,INFO,TIME_MS,STAGE,MAX_STAGE,PROGRESS,MEMORY_USED,MAX_MEMORY_USED,EXAMINED_ROWS,SENT_ROWS,QUERY_ID,INFO_BINARY,TID,TMP_SPACE_USED",
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,UNIQUE_CONSTRAINT_CATALOG,UNIQUE_CONSTRAINT_SCHEMA,UNIQUE_CONSTRAINT_NAME,MATCH_OPTION,UPDATE_RULE,DELETE_RULE,TABLE_NAME,REFERENCED_TABLE_NAME",
    ),
    (
        "ROUTINES",
        "SPECIFIC_NAME,ROUTINE_CATALOG,ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_TYPE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,DTD_IDENTIFIER,ROUTINE_BODY,ROUTINE_DEFINITION,EXTERNAL_NAME,EXTERNAL_LANGUAGE,PARAMETER_STYLE,IS_DETERMINISTIC,SQL_DATA_ACCESS,SQL_PATH,SECURITY_TYPE,CREATED,LAST_ALTERED,SQL_MODE,ROUTINE_COMMENT,DEFINER,CHARACTER_SET_CLIENT,COLLATION_CONNECTION,DATABASE_COLLATION",
    ),
    (
        "SCHEMATA",
        "CATALOG_NAME,SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME,SQL_PATH,SCHEMA_COMMENT",
    ),
    (
        "SCHEMA_PRIVILEGES",
        "GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,PRIVILEGE_TYPE,IS_GRANTABLE",
    ),
    (
        "SEQUENCES",
        "SEQUENCE_CATALOG,SEQUENCE_SCHEMA,SEQUENCE_NAME,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_PRECISION_RADIX,NUMERIC_SCALE,START_VALUE,MINIMUM_VALUE,MAXIMUM_VALUE,INCREMENT,CYCLE_OPTION",
    ),
    (
        "STATISTICS",
        "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,IGNORED",
    ),
    (
        "TABLES",
        "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,VERSION,ROW_FORMAT,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,AUTO_INCREMENT,CREATE_TIME,UPDATE_TIME,CHECK_TIME,TABLE_COLLATION,CHECKSUM,CREATE_OPTIONS,TABLE_COMMENT,MAX_INDEX_LENGTH,TEMPORARY",
    ),
    (
        "TABLE_CONSTRAINTS",
        "CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_TYPE",
    ),
    (
        "TABLE_PRIVILEGES",
        "GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE",
    ),
    (
        "TRIGGERS",
        "TRIGGER_CATALOG,TRIGGER_SCHEMA,TRIGGER_NAME,EVENT_MANIPULATION,EVENT_OBJECT_CATALOG,EVENT_OBJECT_SCHEMA,EVENT_OBJECT_TABLE,ACTION_ORDER,ACTION_CONDITION,ACTION_STATEMENT,ACTION_ORIENTATION,ACTION_TIMING,ACTION_REFERENCE_OLD_TABLE,ACTION_REFERENCE_NEW_TABLE,ACTION_REFERENCE_OLD_ROW,ACTION_REFERENCE_NEW_ROW,CREATED,SQL_MODE,DEFINER,CHARACTER_SET_CLIENT,COLLATION_CONNECTION,DATABASE_COLLATION",
    ),
    (
        "VIEWS",
        "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE,DEFINER,SECURITY_TYPE,CHARACTER_SET_CLIENT,COLLATION_CONNECTION,ALGORITHM",
    ),
];

impl ColumnRegistry {
    /// MariaDB 11.8's column lists, built in.
    #[must_use]
    pub fn mariadb_11_8() -> Self {
        Self::from_rows(MARIADB_11_8.iter().flat_map(|(t, cols)| {
            cols.split(',')
                .map(move |c| ((*t).to_owned(), c.to_owned()))
        }))
    }

    /// The query the gateway runs once per backend version to load the
    /// registry: `(TABLE_NAME, COLUMN_NAME)` rows in order. It names one
    /// schema by equality, so it opens nothing else.
    #[must_use]
    pub const fn load_sql() -> &'static str {
        "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = 'information_schema' ORDER BY TABLE_NAME, ORDINAL_POSITION"
    }

    /// Builds a registry from `(table, column)` pairs in column order.
    pub fn from_rows(rows: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut tables: HashMap<String, Vec<String>> = HashMap::new();
        for (t, c) in rows {
            tables.entry(t.to_ascii_uppercase()).or_default().push(c);
        }
        Self { tables }
    }

    /// The columns of `table`, in order.
    #[must_use]
    pub fn columns(&self, table: &str) -> Option<&[String]> {
        self.tables
            .get(&table.to_ascii_uppercase())
            .map(Vec::as_slice)
    }

    fn require(&self, table: &str) -> Result<&[String], NameError> {
        self.columns(table).ok_or_else(|| {
            NameError::Unsupported(format!(
                "no column list for information_schema.{table} (registry miss)"
            ))
        })
    }
}

/// Extra hiding a table needs beyond name prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Extra {
    None,
    /// `STATISTICS`: indexes with a hidden column.
    Index,
    /// Constraints with a hidden column (by `KEY_COLUMN_USAGE`).
    Constraint,
    /// `TABLE_CONSTRAINTS`: also checks naming a hidden column.
    ConstraintAndCheck,
    /// `CHECK_CONSTRAINTS`: checks naming a hidden column.
    Check,
}

/// How one `information_schema` table is mapped.
#[derive(Debug)]
struct Spec {
    table: &'static str,
    /// The schema column MariaDB's lookup uses; branches restrict it.
    restrict: &'static str,
    /// Columns that always equal `restrict`.
    same: &'static [&'static str],
    /// Other schema columns, mapped with `CASE`.
    schemas: &'static [&'static str],
    /// SQL text columns, mapped with `REPLACE` on backquoted names.
    definitions: &'static [&'static str],
    /// Object-name columns hidden by the `__sprig_` prefix, and whether
    /// they are nullable.
    objects: &'static [(&'static str, bool)],
    /// Column-name columns hidden by the `_sprig_` prefix and the GIPK.
    columns: &'static [&'static str],
    /// The index-name column automatic indexes are hidden by.
    auto_index: Option<&'static str>,
    /// The table-name column whose equalities are copied into branches.
    name: Option<&'static str>,
    extra: Extra,
}

const fn spec(table: &'static str, restrict: &'static str) -> Spec {
    Spec {
        table,
        restrict,
        same: &[],
        schemas: &[],
        definitions: &[],
        objects: &[("TABLE_NAME", false)],
        columns: &[],
        auto_index: None,
        name: Some("TABLE_NAME"),
        extra: Extra::None,
    }
}

const SPECS: &[Spec] = &[
    spec("TABLES", "TABLE_SCHEMA"),
    Spec {
        columns: &["COLUMN_NAME"],
        ..spec("COLUMNS", "TABLE_SCHEMA")
    },
    Spec {
        same: &["INDEX_SCHEMA"],
        auto_index: Some("INDEX_NAME"),
        extra: Extra::Index,
        ..spec("STATISTICS", "TABLE_SCHEMA")
    },
    Spec {
        same: &["CONSTRAINT_SCHEMA"],
        schemas: &["REFERENCED_TABLE_SCHEMA"],
        objects: &[("TABLE_NAME", false), ("REFERENCED_TABLE_NAME", true)],
        extra: Extra::Constraint,
        ..spec("KEY_COLUMN_USAGE", "TABLE_SCHEMA")
    },
    Spec {
        schemas: &["UNIQUE_CONSTRAINT_SCHEMA"],
        objects: &[("TABLE_NAME", false), ("REFERENCED_TABLE_NAME", false)],
        extra: Extra::Constraint,
        ..spec("REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA")
    },
    Spec {
        same: &["CONSTRAINT_SCHEMA"],
        extra: Extra::ConstraintAndCheck,
        ..spec("TABLE_CONSTRAINTS", "TABLE_SCHEMA")
    },
    Spec {
        extra: Extra::Check,
        ..spec("CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA")
    },
    Spec {
        definitions: &["VIEW_DEFINITION"],
        ..spec("VIEWS", "TABLE_SCHEMA")
    },
    Spec {
        definitions: &["ROUTINE_DEFINITION"],
        objects: &[("ROUTINE_NAME", false), ("SPECIFIC_NAME", false)],
        name: None,
        ..spec("ROUTINES", "ROUTINE_SCHEMA")
    },
    Spec {
        objects: &[("SPECIFIC_NAME", false)],
        name: None,
        ..spec("PARAMETERS", "SPECIFIC_SCHEMA")
    },
    Spec {
        same: &["TRIGGER_SCHEMA"],
        definitions: &["ACTION_STATEMENT"],
        objects: &[("TRIGGER_NAME", false), ("EVENT_OBJECT_TABLE", false)],
        name: Some("EVENT_OBJECT_TABLE"),
        ..spec("TRIGGERS", "EVENT_OBJECT_SCHEMA")
    },
    Spec {
        definitions: &["EVENT_DEFINITION"],
        objects: &[("EVENT_NAME", false)],
        name: None,
        ..spec("EVENTS", "EVENT_SCHEMA")
    },
    spec("PARTITIONS", "TABLE_SCHEMA"),
    Spec {
        objects: &[("SEQUENCE_NAME", false)],
        name: None,
        ..spec("SEQUENCES", "SEQUENCE_SCHEMA")
    },
    Spec {
        objects: &[],
        name: None,
        ..spec("SCHEMA_PRIVILEGES", "TABLE_SCHEMA")
    },
    spec("TABLE_PRIVILEGES", "TABLE_SCHEMA"),
    Spec {
        columns: &["COLUMN_NAME"],
        ..spec("COLUMN_PRIVILEGES", "TABLE_SCHEMA")
    },
];

/// Tables without schema names, relayed as they are. The account tables
/// (`*_ROLES`, `USER_PRIVILEGES`) have their account columns mapped in
/// result rows (rule 8). `FILES` is always empty on MariaDB.
const PASS_THROUGH: &[&str] = &[
    "CHARACTER_SETS",
    "COLLATIONS",
    "COLLATION_CHARACTER_SET_APPLICABILITY",
    "ENGINES",
    "KEYWORDS",
    "SQL_FUNCTIONS",
    "PLUGINS",
    "ALL_PLUGINS",
    "PROFILING",
    "SPATIAL_REF_SYS",
    "FILES",
    "ENABLED_ROLES",
    "APPLICABLE_ROLES",
    "USER_PRIVILEGES",
];

/// What happens to a reference to `information_schema.<table>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableClass {
    /// An inline view over the backend's table.
    Mapped,
    /// `SCHEMATA`: the catalog's databases.
    Schemata,
    /// `PROCESSLIST`: the gateway's sessions.
    Processlist,
    /// Relayed unchanged.
    PassThrough,
    /// A SprigDB view (build step 11): 39051 until it exists.
    Sprig,
    /// A server-wide table: 1142.
    Denied,
}

/// Classifies an `information_schema` table name (ignoring case).
#[must_use]
pub fn classify_table(name: &str) -> TableClass {
    let up = name.to_ascii_uppercase();
    if SPECS.iter().any(|s| s.table == up) {
        TableClass::Mapped
    } else if up == "SCHEMATA" {
        TableClass::Schemata
    } else if up == "PROCESSLIST" {
        TableClass::Processlist
    } else if PASS_THROUGH.contains(&up.as_str()) {
        TableClass::PassThrough
    } else if up.starts_with("SPRIG_") {
        TableClass::Sprig
    } else {
        TableClass::Denied
    }
}

/// Everything the `information_schema` rewrite needs.
#[derive(Clone, Copy)]
pub struct InfoSchemaCtx<'a> {
    /// The tenant's names.
    pub map: &'a dyn NameMap,
    /// `sql_mode` flags for literals and the persona.
    pub lex: LexOptions,
    /// What the session may see.
    pub vis: Visibility,
    /// Column lists.
    pub registry: &'a ColumnRegistry,
    /// The gateway's sessions, for `PROCESSLIST`.
    pub sessions: Option<&'a dyn SessionSource>,
    /// Which databases the user may see (chapter 29); all without it.
    pub visible: Option<&'a (dyn Fn(&LogicalDb) -> bool + Sync)>,
    /// The session's current database is `information_schema`, so
    /// unqualified table names in `FROM` are its tables.
    pub in_information_schema: bool,
    /// The user and client address, for 1142 messages.
    pub user: Option<(&'a str, &'a str)>,
    /// The cluster has chapter 07's name lookup
    /// (`__sprig_sys.__sprig_names_v`, [`super::stored`]): `DEFINER` and
    /// `GRANTEE` are mapped to the tenant's user names inside MariaDB, so
    /// no expression over them (`HEX(DEFINER)`, `INSERT ... SELECT`) sees a
    /// backend account. Without it they are only scrubbed on output.
    pub stored_names: bool,
}

impl std::fmt::Debug for InfoSchemaCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaCtx")
            .field("tenant", &self.map.tenant())
            .field("lex", &self.lex)
            .field("vis", &self.vis)
            .field("in_information_schema", &self.in_information_schema)
            .finish_non_exhaustive()
    }
}

/// A rewritten statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfoSchemaRewrite {
    /// The statement to send.
    pub sql: Vec<u8>,
    /// The `information_schema` tables it reads, upper case, in order.
    pub tables: Vec<String>,
    /// Whether any reference was replaced by an inline view (the result is
    /// then scrubbed).
    pub mapped: bool,
}

#[derive(Debug, Clone)]
struct TableRef {
    first: usize,
    last: usize,
    table: String,
    class: TableClass,
    depth: u32,
}

struct Toks<'a> {
    src: &'a [u8],
    t: Vec<Token>,
    depth: Vec<u32>,
    nbe: bool,
}

impl Toks<'_> {
    fn word(&self, j: usize, w: &str) -> bool {
        self.t.get(j).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.src).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn any(&self, j: usize, ws: &[&str]) -> bool {
        ws.iter().any(|w| self.word(j, w))
    }

    fn ident(&self, j: usize) -> bool {
        self.t.get(j).is_some_and(|t| t.kind.is_ident_like())
    }

    fn value(&self, j: usize) -> String {
        self.t.get(j).map_or_else(String::new, |t| {
            String::from_utf8_lossy(&ident_value(t.bytes(self.src))).into_owned()
        })
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t
            .get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && self.src[t.start as usize] == c)
    }

    fn op(&self, j: usize, s: &str) -> bool {
        self.t
            .get(j)
            .is_some_and(|t| t.kind == TokenKind::Op && t.bytes(self.src) == s.as_bytes())
    }

    fn is_string(&self, j: usize) -> bool {
        self.t.get(j).is_some_and(|t| t.kind == TokenKind::String)
    }

    fn text(&self, j: usize) -> &str {
        self.t
            .get(j)
            .and_then(|t| std::str::from_utf8(t.bytes(self.src)).ok())
            .unwrap_or_default()
    }
}

/// Words that end a `FROM` clause at its depth.
const FROM_END: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "INTO",
    "FOR",
    "LOCK",
    "ON",
    "USING",
    "SET",
    "RETURNING",
];

/// Words that end a `WHERE` clause at its depth.
const WHERE_END: &[&str] = &[
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "INTO",
    "FOR",
    "LOCK",
    "RETURNING",
];

/// Words after a table name that are not an alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "STRAIGHT_JOIN",
    "FULL",
    "OUTER",
    "ON",
    "USING",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "FOR",
    "LOCK",
    "INTO",
    "USE",
    "FORCE",
    "IGNORE",
    "PARTITION",
    "SET",
    "RETURNING",
    "OFFSET",
    "FETCH",
    "PROCEDURE",
    "AS",
];

/// Statement kinds that run now, where an inline view is safe.
fn runs_now(kind: StmtKind) -> bool {
    matches!(
        kind,
        StmtKind::Select
            | StmtKind::Insert
            | StmtKind::Replace
            | StmtKind::Update
            | StmtKind::Delete
            | StmtKind::Set
            | StmtKind::Other
            | StmtKind::LoadData
    )
}

/// Rewrites every `information_schema` reference in one statement
/// (rewritten text, physical names). `None` when the statement has none to
/// change.
///
/// # Errors
///
/// 1142 for a server-wide table, 39051 for a `SPRIG_` view, for a mapped
/// table in a statement that doesn't run now (a view or stored program
/// definition), or for a registry miss.
#[allow(clippy::too_many_lines)]
pub fn rewrite_information_schema(
    sql: &[u8],
    kind: StmtKind,
    ctx: &InfoSchemaCtx<'_>,
) -> Result<Option<InfoSchemaRewrite>, NameError> {
    if matches!(kind, StmtKind::Show | StmtKind::Describe) {
        return Ok(None);
    }
    let tokens = lex::lex(sql, &ctx.lex).map_err(|e| NameError::Unsupported(e.to_string()))?;
    let t: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut depth = Vec::with_capacity(t.len());
    let mut d = 0u32;
    for tok in &t {
        if tok.kind == TokenKind::Punct && sql[tok.start as usize] == b')' {
            d = d.saturating_sub(1);
        }
        depth.push(d);
        if tok.kind == TokenKind::Punct && sql[tok.start as usize] == b'(' {
            d += 1;
        }
    }
    let tk = Toks {
        src: sql,
        t,
        depth,
        nbe: ctx.lex.no_backslash_escapes,
    };
    let (refs, cols) = find_refs(&tk, ctx);
    if refs.is_empty() && cols.is_empty() {
        return Ok(None);
    }
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    let mut tables = Vec::new();
    let viewed = refs
        .iter()
        .filter(|r| {
            matches!(
                r.class,
                TableClass::Mapped | TableClass::Schemata | TableClass::Processlist
            )
        })
        .count();
    let local = |table: &str| {
        runs_now(kind)
            .then(|| schema::local_view(table, ctx.lex.persona, &|v| lit(v, ctx)))
            .flatten()
    };
    for r in &refs {
        tables.push(r.table.clone());
        // A table the persona answers in its own shape (chapter 28).
        if let Some(view) = local(&r.table) {
            let (alias, _) = alias_at(&tk, r.last + 1);
            let range = tk.t[r.first].start as usize..tk.t[r.last].end as usize;
            let replacement = if alias.is_some() {
                format!("({view})")
            } else {
                format!("({view}) AS {}", quote(&tk.value(r.last)))
            };
            edits.push((range, replacement));
            continue;
        }
        match r.class {
            TableClass::PassThrough => {
                if !ACCOUNT_TABLES.contains(&r.table.as_str()) {
                    continue;
                }
                // Chapter 07 step 12: backend accounts never reach a
                // definition, where nothing maps them.
                if !runs_now(kind) {
                    return Err(NameError::Unsupported(format!(
                        "information_schema.{} in a view or stored program definition",
                        r.table
                    )));
                }
                let Some(view) = ctx
                    .stored_names
                    .then(|| account_view(&r.table, ctx))
                    .flatten()
                else {
                    continue;
                };
                let (alias, _) = alias_at(&tk, r.last + 1);
                let range = tk.t[r.first].start as usize..tk.t[r.last].end as usize;
                let replacement = if alias.is_some() {
                    format!("({view})")
                } else {
                    format!("({view}) AS {}", quote(&tk.value(r.last)))
                };
                edits.push((range, replacement));
                continue;
            }
            TableClass::Denied => return Err(denied(&r.table, ctx)),
            TableClass::Sprig => {
                return Err(NameError::Unsupported(format!(
                    "information_schema.{} is not available yet",
                    r.table
                )));
            }
            TableClass::Mapped | TableClass::Schemata | TableClass::Processlist => {}
        }
        if !runs_now(kind) {
            return Err(NameError::Unsupported(format!(
                "information_schema.{} in a view or stored program definition",
                r.table
            )));
        }
        let (alias, after) = alias_at(&tk, r.last + 1);
        let alias_name = alias.clone().unwrap_or_else(|| r.table.clone());
        let push = if viewed == 1 {
            pushdown(&tk, r, after, &alias_name)
        } else {
            Pushdown::default()
        };
        let view = match r.class {
            TableClass::Mapped => {
                let spec = SPECS
                    .iter()
                    .find(|s| s.table == r.table)
                    .ok_or_else(|| NameError::Unsupported(r.table.clone()))?;
                mapped_view(spec, &push, ctx)?
            }
            TableClass::Schemata => schemata_view(&push, ctx)?,
            _ => processlist_view(ctx)?,
        };
        let range = tk.t[r.first].start as usize..tk.t[r.last].end as usize;
        let replacement = if alias.is_some() {
            format!("({view})")
        } else {
            format!("({view}) AS {}", quote(&tk.value(r.last)))
        };
        edits.push((range, replacement));
    }
    for &(j, ref table) in &cols {
        if local(table).is_some() {
            edits.push((
                tk.t[j].start as usize..tk.t[j + 1].end as usize,
                String::new(),
            ));
            continue;
        }
        match classify_table(table) {
            TableClass::PassThrough => {}
            TableClass::Denied => return Err(denied(table, ctx)),
            TableClass::Sprig => {
                return Err(NameError::Unsupported(format!(
                    "information_schema.{table} is not available yet"
                )));
            }
            _ => edits.push((
                tk.t[j].start as usize..tk.t[j + 1].end as usize,
                String::new(),
            )),
        }
    }
    if edits.is_empty() {
        return Ok(None);
    }
    edits.sort_by_key(|e| e.0.start);
    let mut out = Vec::with_capacity(sql.len() + 512);
    let mut at = 0;
    for (r, s) in &edits {
        out.extend_from_slice(&sql[at..r.start]);
        out.extend_from_slice(s.as_bytes());
        at = r.end;
    }
    out.extend_from_slice(&sql[at..]);
    Ok(Some(InfoSchemaRewrite {
        sql: out,
        tables,
        mapped: true,
    }))
}

fn denied(table: &str, ctx: &InfoSchemaCtx<'_>) -> NameError {
    let (user, host) = ctx.user.unwrap_or(("", ""));
    NameError::Refused {
        errno: 1142,
        sqlstate: *b"42000",
        message: format!(
            "SELECT command denied to user '{user}'@'{host}' for table `information_schema`.`{table}`"
        ),
    }
}

/// Table references (qualified, or unqualified in `FROM` when the session
/// is in `information_schema`) and three-part column references
/// (`information_schema.T.c`: the schema token index and table).
fn find_refs(tk: &Toks<'_>, ctx: &InfoSchemaCtx<'_>) -> (Vec<TableRef>, Vec<(usize, String)>) {
    let implicit = ctx.in_information_schema;
    let mut refs = Vec::new();
    let mut cols = Vec::new();
    let mut in_from: Vec<bool> = vec![false];
    let n = tk.t.len();
    for j in 0..n {
        let d = tk.depth[j] as usize;
        if in_from.len() <= d + 1 {
            in_from.resize(d + 2, false);
        }
        if tk.punct(j, b'(') {
            in_from[d + 1] = false;
        }
        if tk.word(j, "FROM") || tk.word(j, "JOIN") || tk.word(j, "STRAIGHT_JOIN") {
            in_from[d] = true;
        } else if tk.any(j, FROM_END) || tk.punct(j, b';') {
            in_from[d] = false;
        }
        let after_dot = j > 0 && tk.punct(j - 1, b'.');
        if !tk.ident(j) || after_dot {
            continue;
        }
        if tk.value(j).eq_ignore_ascii_case("information_schema")
            && tk.punct(j + 1, b'.')
            && tk.ident(j + 2)
        {
            let table = tk.value(j + 2).to_ascii_uppercase();
            if tk.punct(j + 3, b'.') && (tk.ident(j + 4) || tk.op(j + 4, "*")) {
                cols.push((j, table));
            } else {
                let class = classify_table(&table);
                refs.push(TableRef {
                    first: j,
                    last: j + 2,
                    table,
                    class,
                    depth: tk.depth[j],
                });
            }
            continue;
        }
        if implicit
            && in_from[d]
            && j > 0
            && (tk.any(j - 1, &["FROM", "JOIN", "STRAIGHT_JOIN"]) || tk.punct(j - 1, b','))
            && !tk.punct(j + 1, b'.')
            && !tk.punct(j + 1, b'(')
        {
            let table = tk.value(j).to_ascii_uppercase();
            let class = classify_table(&table);
            // A name the registry doesn't know may be a CTE or derived
            // table; MariaDB resolves it.
            let known = class != TableClass::Denied
                || table.starts_with("INNODB_")
                || ctx.registry.columns(&table).is_some();
            if known {
                refs.push(TableRef {
                    first: j,
                    last: j,
                    table,
                    class,
                    depth: tk.depth[j],
                });
            }
        }
    }
    (refs, cols)
}

/// The alias after a table name at `k`, and the index after it.
fn alias_at(tk: &Toks<'_>, k: usize) -> (Option<String>, usize) {
    if tk.word(k, "AS") && tk.ident(k + 1) {
        return (Some(tk.value(k + 1)), k + 2);
    }
    match tk.t.get(k) {
        Some(t) if t.kind == TokenKind::QuotedIdent => (Some(tk.value(k)), k + 1),
        Some(t) if t.kind == TokenKind::Word && !tk.any(k, NOT_ALIAS) => (Some(tk.value(k)), k + 1),
        _ => (None, k),
    }
}

/// Predicates pushed into the branches.
#[derive(Debug, Clone, Default)]
struct Pushdown {
    /// Schema values the outer query keeps (`None`: any).
    schemas: Option<Vec<String>>,
    /// `(column, SQL text of the predicate)` on table-name columns.
    names: Vec<(String, String)>,
}

/// One top-level conjunct: a column (maybe qualified), and its values.
struct Conjunct {
    qualifier: Option<String>,
    column: String,
    /// String token indexes.
    values: Vec<usize>,
}

#[allow(clippy::many_single_char_names)]
fn conjunct(tk: &Toks<'_>, a: usize, b: usize) -> Option<Conjunct> {
    let col_at = |k: usize| -> Option<(Option<String>, String, usize)> {
        if tk.ident(k)
            && tk.value(k).eq_ignore_ascii_case("information_schema")
            && tk.punct(k + 1, b'.')
            && tk.ident(k + 2)
            && tk.punct(k + 3, b'.')
            && tk.ident(k + 4)
        {
            // information_schema.T.c, which the rewrite turns into T.c.
            return Some((Some(tk.value(k + 2)), tk.value(k + 4), k + 5));
        }
        if tk.ident(k) && tk.punct(k + 1, b'.') && tk.ident(k + 2) {
            Some((Some(tk.value(k)), tk.value(k + 2), k + 3))
        } else if tk.ident(k) {
            Some((None, tk.value(k), k + 1))
        } else {
            None
        }
    };
    // 'lit' = col
    if tk.is_string(a) && tk.op(a + 1, "=") {
        let (q, c, end) = col_at(a + 2)?;
        return (end == b).then_some(Conjunct {
            qualifier: q,
            column: c,
            values: vec![a],
        });
    }
    let (q, c, k) = col_at(a)?;
    if tk.op(k, "=") && tk.is_string(k + 1) && k + 2 == b {
        return Some(Conjunct {
            qualifier: q,
            column: c,
            values: vec![k + 1],
        });
    }
    if tk.word(k, "IN") && tk.punct(k + 1, b'(') {
        let mut values = Vec::new();
        let mut m = k + 2;
        loop {
            if !tk.is_string(m) {
                return None;
            }
            values.push(m);
            if tk.punct(m + 1, b',') {
                m += 2;
            } else if tk.punct(m + 1, b')') && m + 2 == b {
                break;
            } else {
                return None;
            }
        }
        return Some(Conjunct {
            qualifier: q,
            column: c,
            values,
        });
    }
    None
}

/// Finds the `WHERE` of the query block that holds `r` and reads its
/// top-level conjuncts. Nothing is pushed when the clause has a top-level
/// `OR` or `XOR`.
#[allow(clippy::many_single_char_names)]
fn pushdown(tk: &Toks<'_>, r: &TableRef, from: usize, alias: &str) -> Pushdown {
    let mut out = Pushdown::default();
    let n = tk.t.len();
    let d = r.depth;
    let mut k = from;
    let start = loop {
        if k >= n || tk.depth[k] < d || tk.punct(k, b';') {
            return out;
        }
        if tk.depth[k] == d {
            if tk.word(k, "WHERE") {
                break k + 1;
            }
            if tk.any(k, WHERE_END) {
                return out;
            }
        }
        k += 1;
    };
    let mut end = start;
    while end < n
        && tk.depth[end] >= d
        && !(tk.depth[end] == d && (tk.any(end, WHERE_END) || tk.punct(end, b';')))
    {
        end += 1;
    }
    let mut parts = Vec::new();
    let mut a = start;
    let mut between = false;
    for m in start..end {
        if tk.depth[m] != d {
            continue;
        }
        if tk.any(m, &["OR", "XOR"]) || tk.op(m, "||") {
            return out;
        }
        if tk.word(m, "BETWEEN") {
            between = true;
        } else if tk.word(m, "AND") || tk.op(m, "&&") {
            if between {
                between = false;
                continue;
            }
            parts.push((a, m));
            a = m + 1;
        }
    }
    parts.push((a, end));
    for (a, b) in parts {
        let Some(c) = conjunct(tk, a, b) else {
            continue;
        };
        if c.qualifier
            .as_deref()
            .is_some_and(|q| !q.eq_ignore_ascii_case(alias))
        {
            continue;
        }
        let spec_schema = |col: &str| -> bool {
            let up = col.to_ascii_uppercase();
            match SPECS.iter().find(|s| s.table == r.table) {
                Some(s) => s.restrict == up || s.same.contains(&up.as_str()),
                None => r.table == "SCHEMATA" && up == "SCHEMA_NAME",
            }
        };
        let name_col = SPECS
            .iter()
            .find(|s| s.table == r.table)
            .and_then(|s| s.name);
        if spec_schema(&c.column) {
            let vals: Vec<String> = c
                .values
                .iter()
                .filter_map(|&v| {
                    string_value(tk.t[v].bytes(tk.src), tk.nbe)
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                })
                .collect();
            out.schemas = Some(match out.schemas.take() {
                None => vals,
                Some(prev) => prev
                    .into_iter()
                    .filter(|p| vals.iter().any(|v| v.eq_ignore_ascii_case(p)))
                    .collect(),
            });
        } else if name_col.is_some_and(|nc| nc.eq_ignore_ascii_case(&c.column)) {
            let col = name_col.unwrap_or_default().to_owned();
            let lits: Vec<&str> = c.values.iter().map(|&v| tk.text(v)).collect();
            let sql = if lits.len() == 1 {
                format!("{} = {}", quote(&col), lits[0])
            } else {
                format!("{} IN ({})", quote(&col), lits.join(", "))
            };
            out.names.push((col, sql));
        }
    }
    out
}

fn lit(s: &str, ctx: &InfoSchemaCtx<'_>) -> String {
    sql_string(s, ctx.lex.no_backslash_escapes)
}

/// A logical name in a view, typed as MariaDB types its `information_schema`
/// name columns (`utf8mb3_general_ci`). A bare literal would take the
/// session's collation, and MariaDB 11.8 refuses to compare it with a
/// prepared statement's parameter when the two differ ("Illegal mix of
/// collations", seen with Prisma's introspection through the gateway).
fn name_lit(s: &str, ctx: &InfoSchemaCtx<'_>) -> String {
    format!(
        "CONVERT({} USING utf8mb3) COLLATE utf8mb3_general_ci",
        lit(s, ctx)
    )
}

/// The account tables MariaDB fills with backend accounts: refused inside
/// definitions (chapter 07 rule 9), and their `GRANTEE` mapped at the
/// source with the name lookup.
const ACCOUNT_TABLES: &[&str] = &["USER_PRIVILEGES", "APPLICABLE_ROLES", "ENABLED_ROLES"];

/// `DEFINER` (`t42_u7@%`) as the tenant's account (`alice@%`), computed by
/// MariaDB from the name lookup; an account the lookup doesn't know (the
/// R0 provisioning account) stays for the output scrub.
fn definer_sql(col: &str) -> String {
    format!(
        "CONVERT(IFNULL((SELECT `account` FROM `{sys}`.`{v}` WHERE `name` = \
         SUBSTRING_INDEX({col}, '@', 1)), {col}) USING utf8mb3) COLLATE utf8mb3_general_ci",
        sys = super::stored::SYS_SCHEMA,
        v = super::stored::NAMES_VIEW,
    )
}

/// `GRANTEE` (`'t42_u7'@'%'`) as `'alice'@'%'`, as [`definer_sql`].
fn grantee_sql(col: &str) -> String {
    format!(
        "CONVERT(IFNULL((SELECT CONCAT('''', REPLACE(`logical`, '''', ''''''), '''@''', \
         SUBSTRING_INDEX(`account`, '@', -1), '''') FROM `{sys}`.`{v}` WHERE `name` = \
         SUBSTRING_INDEX(SUBSTRING({col}, 2), '''', 1)), {col}) USING utf8mb3) \
         COLLATE utf8mb3_general_ci",
        sys = super::stored::SYS_SCHEMA,
        v = super::stored::NAMES_VIEW,
    )
}

/// An account table as an inline view with `GRANTEE` mapped.
fn account_view(table: &str, ctx: &InfoSchemaCtx<'_>) -> Option<String> {
    let cols: Vec<String> = match ctx.registry.columns(table) {
        Some(c) => c.to_vec(),
        None => match table {
            "USER_PRIVILEGES" => ["GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"]
                .map(str::to_owned)
                .to_vec(),
            "APPLICABLE_ROLES" => ["GRANTEE", "ROLE_NAME", "IS_GRANTABLE", "IS_DEFAULT"]
                .map(str::to_owned)
                .to_vec(),
            _ => return None,
        },
    };
    if !cols.iter().any(|c| c.eq_ignore_ascii_case("GRANTEE")) {
        return None;
    }
    let list: Vec<String> = cols
        .iter()
        .map(|c| {
            let qc = quote(c);
            if c.eq_ignore_ascii_case("GRANTEE") {
                format!("{} AS {qc}", grantee_sql(&qc))
            } else {
                qc
            }
        })
        .collect();
    Some(format!(
        "SELECT {} FROM information_schema.{table}",
        list.join(", ")
    ))
}

/// Every (physical, logical) pair of the tenant, for `CASE` and `REPLACE`.
fn all_pairs(ctx: &InfoSchemaCtx<'_>) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for db in ctx.map.list() {
        for s in db
            .shards
            .iter()
            .chain(db.reference.iter())
            .chain(ctx.map.grouped_schemas(db.id))
        {
            v.push((s.to_string(), db.name.clone()));
        }
    }
    v
}

/// The databases whose branches the view has: those the pushdown keeps
/// (compared ignoring case, as `information_schema` does), that the user
/// may see; each with its representative schemas.
fn branches(push: &Pushdown, ctx: &InfoSchemaCtx<'_>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for db in ctx.map.list() {
        if ctx.visible.is_some_and(|f| !f(db)) {
            continue;
        }
        if let Some(keep) = &push.schemas
            && !keep.iter().any(|k| k.eq_ignore_ascii_case(&db.name))
        {
            continue;
        }
        if let Ok(set) = ctx.map.physical(&db.name, Purpose::Metadata) {
            for s in set.0 {
                out.push((s.to_string(), db.name.clone()));
            }
        }
    }
    out
}

/// The condition that a column-name column is hidden, or `None` when the
/// session sees every column.
fn hidden_column_cond(col: &str, vis: Visibility) -> Option<String> {
    let mut parts = Vec::new();
    if !vis.internal {
        parts.push(format!("LEFT({}, 7) = '_sprig_'", quote(col)));
        if !vis.gipk {
            parts.push(format!("{} = '{GIPK_COLUMN}'", quote(col)));
        }
    }
    (!parts.is_empty()).then(|| parts.join(" OR "))
}

#[allow(clippy::too_many_lines)]
fn mapped_view(spec: &Spec, push: &Pushdown, ctx: &InfoSchemaCtx<'_>) -> Result<String, NameError> {
    let cols = ctx.registry.require(spec.table)?;
    let pairs = all_pairs(ctx);
    let mut chosen = branches(push, ctx);
    if chosen.is_empty() {
        // No schema of the tenant qualifies: one branch that opens nothing.
        chosen.push(("__sprig_none".to_owned(), String::new()));
    }
    let mysql = (ctx.lex.persona == Persona::MySql84)
        .then(|| schema::mysql_view(spec.table, &|v| lit(v, ctx)))
        .flatten();
    let shape: Vec<(String, Option<String>)> = match &mysql {
        Some(v) => v
            .iter()
            .map(|(n, src)| {
                (
                    (*n).to_owned(),
                    match src {
                        Src::Col(_) => None,
                        Src::Sql(e) => Some(e.clone()),
                    },
                )
            })
            .collect(),
        None => cols.iter().map(|c| (c.clone(), None)).collect(),
    };
    let mut sql = String::with_capacity(512 * chosen.len());
    for (i, (phys, logical)) in chosen.iter().enumerate() {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        let start = sql.len();
        sql.push_str("SELECT ");
        for (k, (c, computed)) in shape.iter().enumerate() {
            if k > 0 {
                sql.push_str(", ");
            }
            let qc = quote(c);
            let e = match computed {
                Some(e) => e.clone(),
                None => column_sql(spec, c, logical, &pairs, ctx),
            };
            if e == qc {
                sql.push_str(&qc);
            } else {
                let _ = write!(sql, "{e} AS {qc}");
            }
        }
        let p = lit(phys, ctx);
        let join = if mysql.is_some() && spec.table == "COLUMNS" {
            schema::json_join(&p, &|v| lit(v, ctx))
        } else {
            String::new()
        };
        let _ = write!(
            sql,
            " FROM information_schema.{}{join} WHERE {} = {}",
            spec.table,
            quote(spec.restrict),
            p
        );
        let vis = &ctx.vis;
        if !vis.internal {
            for (o, nullable) in spec.objects {
                let qo = quote(o);
                if *nullable {
                    let _ = write!(sql, " AND ({qo} IS NULL OR LEFT({qo}, 8) <> '__sprig_')");
                } else {
                    let _ = write!(sql, " AND LEFT({qo}, 8) <> '__sprig_'");
                }
            }
        }
        for c in spec.columns {
            if let Some(cond) = hidden_column_cond(c, *vis) {
                let _ = write!(sql, " AND NOT ({cond})");
            }
        }
        if let Some(ix) = spec.auto_index
            && !vis.auto_indexes
            && !vis.internal
        {
            let _ = write!(sql, " AND LEFT({}, 11) <> 'sprig_auto_'", quote(ix));
        }
        let col_cond = hidden_column_cond("COLUMN_NAME", *vis);
        match (spec.extra, &col_cond) {
            (Extra::Index, Some(cond)) => {
                let _ = write!(
                    sql,
                    " AND (`TABLE_NAME`, `INDEX_NAME`) NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` \
                     FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = {p} AND ({cond}))"
                );
            }
            (Extra::Constraint | Extra::ConstraintAndCheck, Some(cond)) => {
                let _ = write!(
                    sql,
                    " AND (`TABLE_NAME`, `CONSTRAINT_NAME`) NOT IN (SELECT `TABLE_NAME`, \
                     `CONSTRAINT_NAME` FROM information_schema.KEY_COLUMN_USAGE \
                     WHERE `TABLE_SCHEMA` = {p} AND ({cond}))"
                );
            }
            _ => {}
        }
        if !vis.internal {
            match spec.extra {
                Extra::ConstraintAndCheck => {
                    let _ = write!(
                        sql,
                        " AND (`TABLE_NAME`, `CONSTRAINT_NAME`) NOT IN (SELECT `TABLE_NAME`, \
                         `CONSTRAINT_NAME` FROM information_schema.CHECK_CONSTRAINTS \
                         WHERE `CONSTRAINT_SCHEMA` = {p} AND INSTR(`CHECK_CLAUSE`, '`_sprig_') > 0)"
                    );
                }
                Extra::Check => {
                    sql.push_str(" AND INSTR(`CHECK_CLAUSE`, '`_sprig_') = 0");
                }
                _ => {}
            }
        }
        if mysql.is_some()
            && let Some(f) = schema::mysql_branch_filter(spec.table, &p, &|v| lit(v, ctx))
        {
            sql.push_str(&f);
        }
        for (_, cond) in &push.names {
            let _ = write!(sql, " AND {cond}");
        }
        // A schema on another cluster: the rows read there (bugfix-8).
        if let Ok(s) = phys.parse::<SchemaName>()
            && let Some(rows) = ctx.map.remote_branch(&s, &sql[start..])
        {
            sql.push_str(&rows);
        }
    }
    Ok(sql)
}

/// The expression for the backend's column `c` in a branch of `spec`'s
/// view over the schema of `logical`: schema names mapped, definitions
/// unrewritten, other columns as they are.
fn column_sql(
    spec: &Spec,
    c: &str,
    logical: &str,
    pairs: &[(String, String)],
    ctx: &InfoSchemaCtx<'_>,
) -> String {
    let up = c.to_ascii_uppercase();
    let qc = quote(c);
    if up == spec.restrict || spec.same.contains(&up.as_str()) {
        name_lit(logical, ctx)
    } else if spec.schemas.contains(&up.as_str()) {
        if pairs.is_empty() {
            "NULL".to_owned()
        } else {
            let mut e = format!("CASE {qc}");
            for (p, l) in pairs {
                let _ = write!(e, " WHEN {} THEN {}", lit(p, ctx), name_lit(l, ctx));
            }
            e.push_str(" ELSE NULL END");
            e
        }
    } else if ctx.stored_names && up == "DEFINER" {
        definer_sql(&qc)
    } else if ctx.stored_names && up == "GRANTEE" {
        grantee_sql(&qc)
    } else if spec.definitions.contains(&up.as_str()) {
        let mut e = qc;
        for (p, l) in pairs {
            e = format!(
                "REPLACE({e}, {}, {})",
                lit(&quote(p), ctx),
                lit(&quote(l), ctx)
            );
        }
        e
    } else {
        qc
    }
}

fn schemata_view(push: &Pushdown, ctx: &InfoSchemaCtx<'_>) -> Result<String, NameError> {
    let mysql = (ctx.lex.persona == Persona::MySql84)
        .then(|| schema::mysql_view("SCHEMATA", &|v| lit(v, ctx)))
        .flatten();
    let shape: Vec<(String, Option<String>)> = match mysql {
        Some(v) => v
            .into_iter()
            .map(|(n, src)| {
                (
                    n.to_owned(),
                    match src {
                        Src::Col(_) => None,
                        Src::Sql(e) => Some(e),
                    },
                )
            })
            .collect(),
        None => ctx
            .registry
            .require("SCHEMATA")?
            .iter()
            .map(|c| (c.clone(), None))
            .collect(),
    };
    let keep = |name: &str| {
        push.schemas
            .as_ref()
            .is_none_or(|k| k.iter().any(|v| v.eq_ignore_ascii_case(name)))
    };
    let mut parts = Vec::new();
    let names = database_names(ctx.map, ctx.lex.persona, ctx.visible);
    for name in names.iter().filter(|n| keep(n)) {
        let system = super::rewrite::SystemSchema::parse(name).is_some();
        if system {
            let sel: Vec<String> = shape
                .iter()
                .map(|(c, _)| {
                    let v = match c.to_ascii_uppercase().as_str() {
                        "CATALOG_NAME" => "'def'".to_owned(),
                        "SCHEMA_NAME" => name_lit(name, ctx),
                        "DEFAULT_CHARACTER_SET_NAME" => "'utf8mb3'".to_owned(),
                        "DEFAULT_COLLATION_NAME" => "'utf8mb3_general_ci'".to_owned(),
                        "SCHEMA_COMMENT" => "''".to_owned(),
                        "DEFAULT_ENCRYPTION" => "'NO'".to_owned(),
                        _ => "NULL".to_owned(),
                    };
                    format!("{v} AS {}", quote(c))
                })
                .collect();
            parts.push(format!("SELECT {}", sel.join(", ")));
            continue;
        }
        let Some(db) = ctx.map.database(name) else {
            continue;
        };
        let Some(phys) = db.shards.first() else {
            continue;
        };
        let sel: Vec<String> = shape
            .iter()
            .map(|(c, computed)| {
                if c.eq_ignore_ascii_case("SCHEMA_NAME") {
                    format!("{} AS {}", name_lit(name, ctx), quote(c))
                } else if let Some(e) = computed {
                    format!("{e} AS {}", quote(c))
                } else {
                    quote(c)
                }
            })
            .collect();
        parts.push(format!(
            "SELECT {} FROM information_schema.SCHEMATA WHERE `SCHEMA_NAME` = {}",
            sel.join(", "),
            lit(&phys.to_string(), ctx)
        ));
    }
    if parts.is_empty() {
        let sel: Vec<String> = shape
            .iter()
            .map(|(c, _)| format!("NULL AS {}", quote(c)))
            .collect();
        parts.push(format!("SELECT {} FROM DUAL WHERE 0", sel.join(", ")));
    }
    Ok(parts.join(" UNION ALL "))
}

fn processlist_view(ctx: &InfoSchemaCtx<'_>) -> Result<String, NameError> {
    let mysql_cols: Option<Vec<String>> = (ctx.lex.persona == Persona::MySql84)
        .then(|| schema::mysql_column_names("PROCESSLIST"))
        .flatten()
        .map(|v| v.into_iter().map(str::to_owned).collect());
    let cols: &[String] = match &mysql_cols {
        Some(v) => v,
        None => ctx.registry.require("PROCESSLIST")?,
    };
    let rows = ctx
        .sessions
        .ok_or_else(|| {
            NameError::Unsupported("information_schema.PROCESSLIST without sessions".into())
        })?
        .processes();
    let opt = |v: Option<&String>| v.map_or_else(|| "NULL".to_owned(), |s| lit(s, ctx));
    let value = |c: &str, p: &ProcessRow| -> String {
        match c {
            "ID" => p.id.to_string(),
            "USER" => lit(&p.user, ctx),
            "HOST" => lit(&p.host, ctx),
            "DB" => opt(p.db.as_ref()),
            "COMMAND" => lit(&p.command, ctx),
            "TIME" => p.time.to_string(),
            "STATE" => opt(p.state.as_ref()),
            "INFO" | "INFO_BINARY" => opt(p.info.as_ref()),
            "TIME_MS" => format!("{}.000", p.time_ms),
            "PROGRESS" => "0.000".to_owned(),
            "STAGE" | "MAX_STAGE" | "MEMORY_USED" | "MAX_MEMORY_USED" | "EXAMINED_ROWS"
            | "SENT_ROWS" | "QUERY_ID" | "TID" | "TMP_SPACE_USED" => "0".to_owned(),
            _ => "NULL".to_owned(),
        }
    };
    if rows.is_empty() {
        let sel: Vec<String> = cols
            .iter()
            .map(|c| format!("NULL AS {}", quote(c)))
            .collect();
        return Ok(format!("SELECT {} FROM DUAL WHERE 0", sel.join(", ")));
    }
    let parts: Vec<String> = rows
        .iter()
        .map(|p| {
            let sel: Vec<String> = cols
                .iter()
                .map(|c| format!("{} AS {}", value(&c.to_ascii_uppercase(), p), quote(c)))
                .collect();
            format!("SELECT {}", sel.join(", "))
        })
        .collect();
    Ok(parts.join(" UNION ALL "))
}

#[cfg(test)]
mod tests;
