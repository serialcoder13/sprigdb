//! The core `information_schema` providers and `SHOW` rewrites per persona
//! (chapter 28, "Compatibility views"; build step 4).
//!
//! Chapter 07's inline views ([`crate::names::infoschema`]) already turn
//! every `information_schema` reference into a derived table over the
//! backend's view, restricted to the tenant's schemas. This module gives
//! those derived tables the persona's shape:
//!
//! - **MySQL 8.4 columns on 3306** ([`mysql_view`]): `SELECT *` returns
//!   MySQL's column list, and the values follow MySQL. `COLUMNS` reports
//!   `json` for a `LONGTEXT` with MariaDB's JSON check (found through
//!   `CHECK_CONSTRAINTS`, [`json_join`]), drops integer display widths,
//!   shows defaults unquoted with `DEFAULT_GENERATED`, and maps collation
//!   names ([`super::collation`]); `STATISTICS` has `IS_VISIBLE` and
//!   `EXPRESSION`; `TABLE_CONSTRAINTS` has `ENFORCED`; a foreign key's
//!   default rule reads `NO ACTION`, not MariaDB's `RESTRICT`; the JSON checks
//!   disappear from `CHECK_CONSTRAINTS` and `TABLE_CONSTRAINTS`, as MySQL
//!   has none.
//! - **Tables the 3306 persona answers** ([`local_view`]): `KEYWORDS` with
//!   MySQL 8.4's words and `RESERVED` (MySQL Connector/J's
//!   `getSQLKeywords()` reads it), `COLLATIONS` and `CHARACTER_SETS` with
//!   MySQL's names, and the empty `COLUMN_STATISTICS` that `mysqldump` 8.4
//!   reads. 3307 relays MariaDB's tables.
//! - **`SHOW` on 3306**: `SHOW [FULL] COLUMNS` and `DESCRIBE` become
//!   queries over the `COLUMNS` provider ([`show_columns_sql`]), as in
//!   MySQL 8; `SHOW CREATE TABLE` text gets MySQL's spelling
//!   ([`mysql_create_table`]).
//!
//! Everything else keeps MariaDB's shape, which matches MySQL's for the
//! other tables chapter 07 maps.

use std::fmt::Write as _;

use sprig_common::names::quote;
use sprig_proto_mysql::packets::Persona;

use super::collation;
use super::keywords;
use crate::lex::{Token, TokenKind, ident_value, string_value};
use crate::names::map::{NameMap, Purpose};
use crate::names::rewrite::SystemSchema;
use crate::names::show::ShowInfo;

/// Where one column of a persona-shaped view comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Src {
    /// The backend's column of this name, mapped as chapter 07 maps it.
    Col(&'static str),
    /// An SQL expression over the backend's columns.
    Sql(String),
}

/// Quotes a string literal for the session.
pub type Lit<'a> = &'a dyn Fn(&str) -> String;

/// The alias of the JSON-check side table in `COLUMNS` (see [`json_join`]).
const JSON_COL: &str = "`_sprig_jc`";

/// The JSON check's clause for the constraint named by `name_expr`.
fn json_clause(name_expr: &str, lit: Lit<'_>) -> String {
    format!(
        "CONCAT({}, REPLACE({name_expr}, {}, {}), {})",
        lit("json_valid(`"),
        lit("`"),
        lit("``"),
        lit("`)")
    )
}

/// Whether a `COLUMNS` row is a JSON column: `LONGTEXT` with the check.
fn is_json(lit: Lit<'_>) -> String {
    format!(
        "(`DATA_TYPE` = {} AND {JSON_COL} IS NOT NULL)",
        lit("longtext")
    )
}

/// The `LEFT JOIN` that finds the JSON checks of one schema, for the
/// `COLUMNS` branch restricted to `phys` (a quoted literal). Its columns
/// have names `COLUMNS` doesn't, so the branch's other references stay
/// unqualified. The `LIMIT` keeps MariaDB from merging the derived table
/// into the outer join, where its schema equality would no longer narrow
/// the `information_schema` scan to one schema (`EXPLAIN` showed "Scanned
/// all databases").
#[must_use]
pub fn json_join(phys: &str, lit: Lit<'_>) -> String {
    format!(
        " LEFT JOIN (SELECT `TABLE_NAME` AS `_sprig_jt`, `CONSTRAINT_NAME` AS {JSON_COL} \
         FROM information_schema.CHECK_CONSTRAINTS WHERE `CONSTRAINT_SCHEMA` = {phys} \
         AND `LEVEL` = {} AND `CHECK_CLAUSE` = {} LIMIT 4294967295) AS `_sprig_j` \
         ON `_sprig_jt` = `TABLE_NAME` AND {JSON_COL} = `COLUMN_NAME` \
         LEFT JOIN (SELECT `TABLE_NAME` AS `_sprig_nt`, `CONSTRAINT_NAME` AS {NN_COL} \
         FROM information_schema.CHECK_CONSTRAINTS WHERE `CONSTRAINT_SCHEMA` = {phys} \
         AND `LEVEL` = {} AND `CONSTRAINT_NAME` LIKE {} LIMIT 4294967295) AS `_sprig_n` \
         ON `_sprig_nt` = `TABLE_NAME` AND {NN_COL} = CONCAT({}, `COLUMN_NAME`)",
        lit("Column"),
        json_clause("`CONSTRAINT_NAME`", lit),
        lit("Table"),
        lit(&nn_like()),
        lit(super::ddl::NN_PREFIX),
    )
}

/// A `LIKE` pattern for [`super::ddl::NN_PREFIX`] names.
fn nn_like() -> String {
    format!("{}%", super::ddl::NN_PREFIX.replace('_', "\\_"))
}

/// The alias of the generated-`NOT NULL` side table in `COLUMNS` (see
/// [`json_join`] and [`super::ddl`]).
const NN_COL: &str = "`_sprig_nc`";

/// `IS_NULLABLE`: `NO` for a generated column whose `NOT NULL` is a
/// [`super::ddl::NN_PREFIX`] check.
fn is_nullable(lit: Lit<'_>) -> String {
    format!("IF({NN_COL} IS NULL, `IS_NULLABLE`, {})", lit("NO"))
}

/// The alias of the functional-key-part side table in `STATISTICS` (see
/// [`fx_join`]).
const FX_COL: &str = "`_sprig_fc`";

/// The `LEFT JOIN` that finds the hidden columns of functional key parts
/// ([`super::fxindex`]) of one schema and their expressions, for the
/// `STATISTICS` branch restricted to `phys` (a quoted literal).
#[must_use]
pub fn fx_join(phys: &str, lit: Lit<'_>) -> String {
    format!(
        " LEFT JOIN (SELECT `TABLE_NAME` AS `_sprig_ft`, `COLUMN_NAME` AS {FX_COL}, \
         `GENERATION_EXPRESSION` AS `_sprig_fe` FROM information_schema.COLUMNS \
         WHERE `TABLE_SCHEMA` = {phys} AND `COLUMN_NAME` LIKE {} LIMIT 4294967295) AS `_sprig_f` \
         ON `_sprig_ft` = `TABLE_NAME` AND {FX_COL} = `COLUMN_NAME`",
        lit(&format!(
            "{}%",
            super::fxindex::FX_PREFIX.replace('_', "\\_")
        ))
    )
}

/// A functional key part's expression as MySQL prints it: MariaDB's
/// generation expression with MySQL's names for `lcase` and `ucase`.
fn fx_expression(lit: Lit<'_>) -> String {
    format!(
        "REPLACE(REPLACE(`_sprig_fe`, {}, {}), {}, {})",
        lit("lcase("),
        lit("lower("),
        lit("ucase("),
        lit("upper(")
    )
}

/// `SHOW CREATE TABLE` text with functional key parts written back as
/// MySQL writes them (3306, before chapter 07's filter hides `_sprig_`
/// columns): each key's `` `_sprig_fx_..` `` part becomes `(expr)` and the
/// hidden column's line goes.
#[must_use]
pub fn mysql_functional_parts(text: &str) -> String {
    if !text.contains(super::fxindex::FX_PREFIX) {
        return text.to_owned();
    }
    let mut exprs: Vec<(String, String)> = Vec::new();
    for line in text.split('\n') {
        let Some((name, at)) = column_line_name(line) else {
            continue;
        };
        if !super::fxindex::is_fx_column(&name) {
            continue;
        }
        let rest = &line[at..];
        let Some(g) = rest.find(" GENERATED ALWAYS AS (") else {
            continue;
        };
        let body = &rest[g + " GENERATED ALWAYS AS ".len()..];
        let Some(v) = body.rfind(") VIRTUAL") else {
            continue;
        };
        // R4 round 4: MySQL's printing (gaps row 64).
        let raw = &body[1..v];
        let e = super::exprprint::mysql_expr(raw, super::exprprint::table_charset(text))
            .unwrap_or_else(|| raw.replace("lcase(", "lower(").replace("ucase(", "upper("));
        exprs.push((format!("`{}`", name.replace('`', "``")), e));
    }
    if exprs.is_empty() {
        return text.to_owned();
    }
    text.split('\n')
        .filter(|line| {
            !column_line_name(line).is_some_and(|(n, _)| super::fxindex::is_fx_column(&n))
        })
        .map(|line| {
            let t = line.trim_start();
            if t.starts_with("KEY ") || t.starts_with("UNIQUE KEY ") {
                let mut l = line.to_owned();
                for (q, e) in &exprs {
                    l = l.replace(q.as_str(), &format!("({e})"));
                }
                l
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `COLUMN_TYPE` without integer display widths, as MySQL 8.4 shows it
/// (`tinyint(1)` and `ZEROFILL` columns keep theirs).
fn column_type(expr: &str, lit: Lit<'_>) -> String {
    format!(
        "CASE WHEN {expr} LIKE {} OR {expr} LIKE {} THEN {expr} \
         ELSE REGEXP_REPLACE({expr}, {}, {}) END",
        lit("tinyint(1)%"),
        lit("%zerofill"),
        lit(r"^(tinyint|smallint|mediumint|int|bigint|year)\([0-9]+\)"),
        lit(r"\1")
    )
}

/// `COLUMN_DEFAULT` as MySQL shows it: literals unquoted, no default and
/// `DEFAULT NULL` both `NULL`, `CURRENT_TIMESTAMP` in capitals.
fn column_default(lit: Lit<'_>) -> String {
    let d = "`COLUMN_DEFAULT`";
    format!(
        "CASE WHEN {d} IS NULL OR {d} = {null} THEN NULL \
         WHEN {d} LIKE {quoted} THEN REPLACE(SUBSTRING({d}, 2, CHAR_LENGTH({d}) - 2), {two}, {one}) \
         WHEN {d} = {now} THEN {now_mysql} \
         WHEN {d} REGEXP {now_n} THEN UPPER({d}) \
         ELSE {d} END",
        null = lit("NULL"),
        quoted = lit("'%'"),
        two = lit("''"),
        one = lit("'"),
        now = lit("current_timestamp()"),
        now_mysql = lit("CURRENT_TIMESTAMP"),
        now_n = lit(r"^current_timestamp\([0-6]\)$"),
    )
}

/// `EXTRA` as MySQL shows it: `DEFAULT_GENERATED` for an expression
/// default, `CURRENT_TIMESTAMP` in capitals.
fn extra(lit: Lit<'_>) -> String {
    let d = "`COLUMN_DEFAULT`";
    format!(
        "TRIM(CONCAT(IF({d} IS NOT NULL AND {d} <> {null} AND {d} NOT LIKE {quoted} \
         AND {d} NOT REGEXP {number} AND {d} NOT REGEXP {bits}, {generated}, {empty}), \
         REPLACE(REGEXP_REPLACE(`EXTRA`, {now}, {now_mysql}), {empty_call}, {bare})))",
        null = lit("NULL"),
        quoted = lit("'%"),
        number = lit(r"^[-+]?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$"),
        bits = lit("^[bBxX]'"),
        generated = lit("DEFAULT_GENERATED "),
        empty = lit(""),
        now = lit(r"current_timestamp\(([0-6]?)\)"),
        now_mysql = lit(r"CURRENT_TIMESTAMP(\1)"),
        empty_call = lit("CURRENT_TIMESTAMP()"),
        bare = lit("CURRENT_TIMESTAMP"),
    )
}

/// MySQL 8.4's columns of an `information_schema` table whose shape
/// differs from MariaDB 11.8's, with where each comes from. `None`: the
/// shapes match (or the table isn't one chapter 07 maps).
#[must_use]
#[allow(clippy::too_many_lines)] // one column list per table
pub fn mysql_view(table: &str, lit: Lit<'_>) -> Option<Vec<(&'static str, Src)>> {
    use Src::{Col, Sql};
    let json = is_json(lit);
    let coll = |e: &str| collation::sql_case(e, lit);
    Some(match table {
        "COLUMNS" => vec![
            ("TABLE_CATALOG", Col("TABLE_CATALOG")),
            ("TABLE_SCHEMA", Col("TABLE_SCHEMA")),
            ("TABLE_NAME", Col("TABLE_NAME")),
            ("COLUMN_NAME", Col("COLUMN_NAME")),
            ("ORDINAL_POSITION", Col("ORDINAL_POSITION")),
            ("COLUMN_DEFAULT", Sql(column_default(lit))),
            ("IS_NULLABLE", Sql(is_nullable(lit))),
            (
                "DATA_TYPE",
                Sql(format!("IF({json}, {}, `DATA_TYPE`)", lit("json"))),
            ),
            (
                "CHARACTER_MAXIMUM_LENGTH",
                Sql(format!("IF({json}, NULL, `CHARACTER_MAXIMUM_LENGTH`)")),
            ),
            (
                "CHARACTER_OCTET_LENGTH",
                Sql(format!("IF({json}, NULL, `CHARACTER_OCTET_LENGTH`)")),
            ),
            ("NUMERIC_PRECISION", Col("NUMERIC_PRECISION")),
            ("NUMERIC_SCALE", Col("NUMERIC_SCALE")),
            ("DATETIME_PRECISION", Col("DATETIME_PRECISION")),
            (
                "CHARACTER_SET_NAME",
                Sql(format!("IF({json}, NULL, `CHARACTER_SET_NAME`)")),
            ),
            (
                "COLLATION_NAME",
                Sql(format!("IF({json}, NULL, {})", coll("`COLLATION_NAME`"))),
            ),
            (
                "COLUMN_TYPE",
                Sql(format!(
                    "IF({json}, {}, {})",
                    lit("json"),
                    column_type("`COLUMN_TYPE`", lit)
                )),
            ),
            ("COLUMN_KEY", Col("COLUMN_KEY")),
            ("EXTRA", Sql(extra(lit))),
            ("PRIVILEGES", Col("PRIVILEGES")),
            ("COLUMN_COMMENT", Col("COLUMN_COMMENT")),
            (
                "GENERATION_EXPRESSION",
                Sql(format!("IFNULL(`GENERATION_EXPRESSION`, {})", lit(""))),
            ),
            ("SRS_ID", Sql("CAST(NULL AS UNSIGNED)".to_owned())),
        ],
        "TABLES" => {
            let mut v: Vec<(&'static str, Src)> = [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "TABLE_TYPE",
                "ENGINE",
                "VERSION",
                "ROW_FORMAT",
                "TABLE_ROWS",
                "AVG_ROW_LENGTH",
                "DATA_LENGTH",
                "MAX_DATA_LENGTH",
                "INDEX_LENGTH",
                "DATA_FREE",
                "AUTO_INCREMENT",
                "CREATE_TIME",
                "UPDATE_TIME",
                "CHECK_TIME",
            ]
            .into_iter()
            .map(|c| (c, Col(c)))
            .collect();
            v.push(("TABLE_COLLATION", Sql(coll("`TABLE_COLLATION`"))));
            v.extend(
                ["CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"]
                    .into_iter()
                    .map(|c| (c, Col(c))),
            );
            v
        }
        "STATISTICS" => {
            let mut v: Vec<(&'static str, Src)> = [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "NON_UNIQUE",
                "INDEX_SCHEMA",
                "INDEX_NAME",
                "SEQ_IN_INDEX",
                "COLUMN_NAME",
                "COLLATION",
                "CARDINALITY",
                "SUB_PART",
                "PACKED",
                "NULLABLE",
                "INDEX_TYPE",
                "COMMENT",
                "INDEX_COMMENT",
            ]
            .into_iter()
            .map(|c| (c, Col(c)))
            .collect();
            v.push((
                "IS_VISIBLE",
                Sql(format!(
                    "IF(`IGNORED` = {}, {}, {})",
                    lit("YES"),
                    lit("NO"),
                    lit("YES")
                )),
            ));
            // Functional key parts (R4 round 3, [`super::fxindex`]):
            // no column, and the expression (see [`fx_join`]).
            v[7] = (
                "COLUMN_NAME",
                Sql(format!("IF({FX_COL} IS NULL, `COLUMN_NAME`, NULL)")),
            );
            v.push(("EXPRESSION", Sql(fx_expression(lit))));
            v
        }
        "TABLE_CONSTRAINTS" => {
            let mut v: Vec<(&'static str, Src)> = [
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "CONSTRAINT_TYPE",
            ]
            .into_iter()
            .map(|c| (c, Col(c)))
            .collect();
            v.push(("ENFORCED", Sql(lit("YES"))));
            v
        }
        // MariaDB reports a rule the DDL left out as RESTRICT, MySQL as NO
        // ACTION; InnoDB treats the two alike, and an explicit RESTRICT
        // can't be told apart on MariaDB, so both read NO ACTION.
        "REFERENTIAL_CONSTRAINTS" => {
            let rule = |c: &str| {
                Sql(format!(
                    "IF(`{c}` = {}, {}, `{c}`)",
                    lit("RESTRICT"),
                    lit("NO ACTION")
                ))
            };
            vec![
                ("CONSTRAINT_CATALOG", Col("CONSTRAINT_CATALOG")),
                ("CONSTRAINT_SCHEMA", Col("CONSTRAINT_SCHEMA")),
                ("CONSTRAINT_NAME", Col("CONSTRAINT_NAME")),
                (
                    "UNIQUE_CONSTRAINT_CATALOG",
                    Col("UNIQUE_CONSTRAINT_CATALOG"),
                ),
                ("UNIQUE_CONSTRAINT_SCHEMA", Col("UNIQUE_CONSTRAINT_SCHEMA")),
                ("UNIQUE_CONSTRAINT_NAME", Col("UNIQUE_CONSTRAINT_NAME")),
                ("MATCH_OPTION", Col("MATCH_OPTION")),
                ("UPDATE_RULE", rule("UPDATE_RULE")),
                ("DELETE_RULE", rule("DELETE_RULE")),
                ("TABLE_NAME", Col("TABLE_NAME")),
                ("REFERENCED_TABLE_NAME", Col("REFERENCED_TABLE_NAME")),
            ]
        }
        "CHECK_CONSTRAINTS" => vec![
            ("CONSTRAINT_CATALOG", Col("CONSTRAINT_CATALOG")),
            ("CONSTRAINT_SCHEMA", Col("CONSTRAINT_SCHEMA")),
            ("CONSTRAINT_NAME", Col("CONSTRAINT_NAME")),
            (
                "CHECK_CLAUSE",
                Sql(format!(
                    "CONCAT({}, `CHECK_CLAUSE`, {})",
                    lit("("),
                    lit(")")
                )),
            ),
        ],
        "VIEWS" => [
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "VIEW_DEFINITION",
            "CHECK_OPTION",
            "IS_UPDATABLE",
            "DEFINER",
            "SECURITY_TYPE",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
        ]
        .into_iter()
        .map(|c| (c, Col(c)))
        .collect(),
        "SCHEMATA" => vec![
            ("CATALOG_NAME", Col("CATALOG_NAME")),
            ("SCHEMA_NAME", Col("SCHEMA_NAME")),
            (
                "DEFAULT_CHARACTER_SET_NAME",
                Col("DEFAULT_CHARACTER_SET_NAME"),
            ),
            (
                "DEFAULT_COLLATION_NAME",
                Sql(coll("`DEFAULT_COLLATION_NAME`")),
            ),
            ("SQL_PATH", Col("SQL_PATH")),
            ("DEFAULT_ENCRYPTION", Sql(lit("NO"))),
        ],
        "PROCESSLIST" => [
            "ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO",
        ]
        .into_iter()
        .map(|c| (c, Col(c)))
        .collect(),
        _ => return None,
    })
}

/// The MySQL view's column names, for `SELECT *` shapes.
#[must_use]
pub fn mysql_column_names(table: &str) -> Option<Vec<&'static str>> {
    mysql_view(table, &|s| format!("'{s}'")).map(|v| v.into_iter().map(|(n, _)| n).collect())
}

/// The extra branch condition of a MySQL-shaped view, for the branch
/// restricted to `phys` (a quoted literal): the JSON checks, which MySQL
/// doesn't have, leave `CHECK_CONSTRAINTS` and `TABLE_CONSTRAINTS`.
#[must_use]
pub fn mysql_branch_filter(table: &str, phys: &str, lit: Lit<'_>) -> Option<String> {
    match table {
        "CHECK_CONSTRAINTS" => Some(format!(
            " AND NOT (`LEVEL` = {} AND `CHECK_CLAUSE` = {}) AND `CONSTRAINT_NAME` NOT LIKE {}",
            lit("Column"),
            json_clause("`CONSTRAINT_NAME`", lit),
            lit(&nn_like())
        )),
        "TABLE_CONSTRAINTS" => Some(format!(
            " AND NOT (`CONSTRAINT_TYPE` = {} AND (`TABLE_NAME`, `CONSTRAINT_NAME`) IN \
             (SELECT `TABLE_NAME`, `CONSTRAINT_NAME` FROM information_schema.CHECK_CONSTRAINTS \
             WHERE `CONSTRAINT_SCHEMA` = {phys} AND `LEVEL` = {} AND `CHECK_CLAUSE` = {})) \
             AND NOT (`CONSTRAINT_TYPE` = {} AND `CONSTRAINT_NAME` LIKE {})",
            lit("CHECK"),
            lit("Column"),
            json_clause("`CONSTRAINT_NAME`", lit),
            lit("CHECK"),
            lit(&nn_like())
        )),
        _ => None,
    }
}

/// The JSON array of MySQL 8.4's keywords for `JSON_TABLE`:
/// `[["ACCESSIBLE",1],...]`.
fn keywords_json() -> String {
    let mut s = String::with_capacity(16 * keywords::MYSQL84.len());
    s.push('[');
    let mut first = true;
    for (w, r) in keywords::MYSQL84 {
        if !first {
            s.push(',');
        }
        first = false;
        let _ = write!(s, "[\"{w}\",{}]", u8::from(*r));
    }
    s.push(']');
    s
}

/// MySQL 8.4's collation names as an `IN` list (see
/// [`collation::MYSQL84_NAMES`]).
fn mysql_collation_list(lit: Lit<'_>) -> String {
    let mut s = String::with_capacity(collation::MYSQL84_NAMES.len() * 24);
    for (i, n) in collation::MYSQL84_NAMES.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&lit(n));
    }
    s
}

/// A table the persona answers itself, as the SQL of a derived table
/// (without parentheses or alias); `None` when the backend's table serves.
#[must_use]
pub fn local_view(table: &str, persona: Persona, lit: Lit<'_>) -> Option<String> {
    match (table, persona) {
        ("KEYWORDS", Persona::MySql84) => Some(format!(
            "SELECT `WORD`, `RESERVED` FROM JSON_TABLE({}, {} COLUMNS (`WORD` VARCHAR(128) \
             PATH {}, `RESERVED` INT PATH {})) AS `_sprig_k`",
            lit(&keywords_json()),
            lit("$[*]"),
            lit("$[0]"),
            lit("$[1]")
        )),
        ("COLUMN_STATISTICS", Persona::MySql84) => Some(
            "SELECT CAST(NULL AS CHAR(64)) AS `SCHEMA_NAME`, CAST(NULL AS CHAR(64)) AS \
             `TABLE_NAME`, CAST(NULL AS CHAR(64)) AS `COLUMN_NAME`, CAST(NULL AS CHAR) AS \
             `HISTOGRAM` FROM DUAL WHERE 0"
                .to_owned(),
        ),
        ("COLLATIONS", Persona::MySql84) => Some(format!(
            "SELECT `COLLATION_NAME`, `CHARACTER_SET_NAME`, `ID`, \
             IF(`CHARACTER_SET_NAME` = {mb4}, IF(`COLLATION_NAME` = {def}, {yes}, {empty}), \
             `IS_DEFAULT`) AS `IS_DEFAULT`, `IS_COMPILED`, `SORTLEN`, \
             IF(`COLLATION_NAME` LIKE {o900}, {nopad}, {pad}) AS `PAD_ATTRIBUTE` \
             FROM information_schema.COLLATIONS WHERE `COLLATION_NAME` IN ({names})",
            mb4 = lit("utf8mb4"),
            def = lit(collation::MYSQL_DEFAULT),
            yes = lit("Yes"),
            empty = lit(""),
            o900 = lit("%0900%"),
            nopad = lit("NO PAD"),
            pad = lit("PAD SPACE"),
            names = mysql_collation_list(lit),
        )),
        ("CHARACTER_SETS", Persona::MySql84) => Some(format!(
            "SELECT `CHARACTER_SET_NAME`, IF(`CHARACTER_SET_NAME` = {}, {}, \
             `DEFAULT_COLLATE_NAME`) AS `DEFAULT_COLLATE_NAME`, `DESCRIPTION`, `MAXLEN` \
             FROM information_schema.CHARACTER_SETS",
            lit("utf8mb4"),
            lit(collation::MYSQL_DEFAULT)
        )),
        ("COLLATION_CHARACTER_SET_APPLICABILITY", Persona::MySql84) => Some(format!(
            "SELECT `COLLATION_NAME`, `CHARACTER_SET_NAME` FROM \
             information_schema.COLLATION_CHARACTER_SET_APPLICABILITY \
             WHERE `COLLATION_NAME` IN ({})",
            mysql_collation_list(lit)
        )),
        _ => None,
    }
}

/// `SHOW [FULL] COLUMNS` (and `DESCRIBE`) on 3306 as a query over the
/// MySQL-shaped `COLUMNS` provider, for `table` in the physical schema
/// `schema`. `like` is the `LIKE` pattern; `filter` the client's
/// `WHERE ...` text, which names the result's columns. The derived table
/// also selects nothing from the table itself, so a missing table fails
/// with 1146 as `SHOW COLUMNS` does.
#[must_use]
pub fn show_columns_sql(
    schema: &str,
    table: &str,
    full: bool,
    like: Option<&str>,
    filter: Option<&[u8]>,
    lit: Lit<'_>,
) -> Vec<u8> {
    let json = is_json(lit);
    let phys = lit(schema);
    let ty = format!(
        "IF({json}, {}, {})",
        lit("json"),
        column_type("`COLUMN_TYPE`", lit)
    );
    let coll = format!(
        "IF({json}, NULL, {})",
        collation::sql_case("`COLLATION_NAME`", lit)
    );
    let mut inner = format!(
        "SELECT `COLUMN_NAME` AS `Field`, {ty} AS `Type`, {coll} AS `Collation`, \
         {} AS `Null`, `COLUMN_KEY` AS `Key`, {} AS `Default`, {} AS `Extra`, \
         `PRIVILEGES` AS `Privileges`, `COLUMN_COMMENT` AS `Comment`, \
         `ORDINAL_POSITION` AS `_sprig_pos` FROM information_schema.COLUMNS",
        is_nullable(lit),
        column_default(lit),
        extra(lit)
    );
    inner.push_str(&json_join(&phys, lit));
    let _ = write!(
        inner,
        " WHERE `TABLE_SCHEMA` = {phys} AND `TABLE_NAME` = {} \
         AND (SELECT 1 FROM {}.{} LIMIT 0) IS NULL",
        lit(table),
        quote(schema),
        quote(table)
    );
    let select = if full {
        "`Field`, `Type`, `Collation`, `Null`, `Key`, `Default`, `Extra`, `Privileges`, `Comment`"
    } else {
        "`Field`, `Type`, `Null`, `Key`, `Default`, `Extra`"
    };
    let mut sql = format!("SELECT {select} FROM ({inner}) AS `COLUMNS`").into_bytes();
    if let Some(f) = filter {
        sql.push(b' ');
        sql.extend_from_slice(f);
    } else if let Some(p) = like {
        sql.extend_from_slice(format!(" WHERE `Field` LIKE {}", lit(p)).as_bytes());
    }
    sql.extend_from_slice(b" ORDER BY `_sprig_pos`");
    sql
}

/// `SHOW INDEX` on 3306 as a query over MariaDB's `STATISTICS` for
/// `table` in the physical schema `schema`, in MySQL 8.4's shape:
/// `Visible` (MariaDB's `Ignored`, inverted) and `Expression` (`NULL`:
/// functional key parts aren't emulated yet) instead of `Ignored`. As for
/// [`show_columns_sql`], the derived table reads nothing from the table,
/// so a missing table fails with 1146. `filter` is the client's `WHERE
/// ...` text over the result's columns.
#[must_use]
pub fn show_index_sql(schema: &str, table: &str, filter: Option<&[u8]>, lit: Lit<'_>) -> Vec<u8> {
    let inner = format!(
        "SELECT `TABLE_NAME` AS `Table`, `NON_UNIQUE` AS `Non_unique`, \
         `INDEX_NAME` AS `Key_name`, `SEQ_IN_INDEX` AS `Seq_in_index`, \
         IF({fx} IS NULL, `COLUMN_NAME`, NULL) AS `Column_name`, `COLLATION` AS `Collation`, \
         `CARDINALITY` AS `Cardinality`, `SUB_PART` AS `Sub_part`, `PACKED` AS `Packed`, \
         `NULLABLE` AS `Null`, `INDEX_TYPE` AS `Index_type`, `COMMENT` AS `Comment`, \
         `INDEX_COMMENT` AS `Index_comment`, IF(`IGNORED` = {yes}, {no}, {yes}) AS `Visible`, \
         {expr} AS `Expression` FROM information_schema.STATISTICS{join} \
         WHERE `TABLE_SCHEMA` = {} AND `TABLE_NAME` = {} \
         AND (SELECT 1 FROM {}.{} LIMIT 0) IS NULL",
        lit(schema),
        lit(table),
        quote(schema),
        quote(table),
        yes = lit("YES"),
        no = lit("NO"),
        fx = FX_COL,
        expr = fx_expression(lit),
        join = fx_join(&lit(schema), lit),
    );
    match filter {
        Some(f) => {
            let mut sql = format!("SELECT * FROM ({inner}) AS `STATISTICS` ").into_bytes();
            sql.extend_from_slice(f);
            sql
        }
        None => inner.into_bytes(),
    }
}

/// MySQL 8.4's `SHOW ENGINES` rows, in its order: name, comment, and
/// `Transactions`, `XA`, `Savepoints` (`None` where MySQL prints `NULL`).
/// Captured from MySQL 8.4.11 on 10 November 2026.
const MYSQL_ENGINES: &[(&str, &str, Option<[&str; 3]>)] = &[
    ("ndbcluster", "Clustered, fault-tolerant tables", None),
    (
        "MEMORY",
        "Hash based, stored in memory, useful for temporary tables",
        Some(["NO", "NO", "NO"]),
    ),
    (
        "InnoDB",
        "Supports transactions, row-level locking, and foreign keys",
        Some(["YES", "YES", "YES"]),
    ),
    (
        "PERFORMANCE_SCHEMA",
        "Performance Schema",
        Some(["NO", "NO", "NO"]),
    ),
    ("MyISAM", "MyISAM storage engine", Some(["NO", "NO", "NO"])),
    ("FEDERATED", "Federated MySQL storage engine", None),
    (
        "ndbinfo",
        "MySQL Cluster system information storage engine",
        None,
    ),
    (
        "MRG_MYISAM",
        "Collection of identical MyISAM tables",
        Some(["NO", "NO", "NO"]),
    ),
    (
        "BLACKHOLE",
        "/dev/null storage engine (anything you write to it disappears)",
        Some(["NO", "NO", "NO"]),
    ),
    ("CSV", "CSV storage engine", Some(["NO", "NO", "NO"])),
    (
        "ARCHIVE",
        "Archive storage engine",
        Some(["NO", "NO", "NO"]),
    ),
];

/// Whether `text` is `SHOW [STORAGE] ENGINES` (MySQL takes no filter).
#[must_use]
pub fn is_show_engines(text: &[u8]) -> bool {
    let words: Vec<&[u8]> = text
        .split(|c: &u8| c.is_ascii_whitespace() || *c == b';')
        .filter(|w| !w.is_empty())
        .collect();
    match words.as_slice() {
        [show, engines] => {
            show.eq_ignore_ascii_case(b"SHOW") && engines.eq_ignore_ascii_case(b"ENGINES")
        }
        [show, storage, engines] => {
            show.eq_ignore_ascii_case(b"SHOW")
                && storage.eq_ignore_ascii_case(b"STORAGE")
                && engines.eq_ignore_ascii_case(b"ENGINES")
        }
        _ => false,
    }
}

/// `SHOW ENGINES` on 3306 (gaps row 48): MySQL 8.4's engines, names and
/// comments, in its order, with `Support` from the backend's own
/// `ENGINES` table: an engine MariaDB doesn't have reads `NO` (and `NULL`
/// for the three capability columns, as MySQL shows a missing one), so
/// a client never picks an engine `CREATE TABLE` would refuse.
/// MariaDB-only engines (`Aria`, `SEQUENCE`) aren't listed.
#[must_use]
pub fn show_engines_sql(lit: Lit<'_>) -> Vec<u8> {
    let null = "CAST(NULL AS CHAR)";
    let mut rows = String::new();
    for (i, (name, comment, caps)) in MYSQL_ENGINES.iter().enumerate() {
        if i > 0 {
            rows.push_str(" UNION ALL ");
        }
        let [t, x, s] = match caps {
            Some(c) => c.map(lit),
            None => [null.to_owned(), null.to_owned(), null.to_owned()],
        };
        let _ = write!(
            rows,
            "SELECT {i} AS `n`, {} AS `e`, {} AS `c`, {t} AS `t`, {x} AS `x`, {s} AS `s`",
            lit(name),
            lit(comment)
        );
    }
    format!(
        "SELECT m.`e` AS `Engine`, IFNULL(b.`SUPPORT`, {no}) AS `Support`, m.`c` AS `Comment`, \
         IF(b.`SUPPORT` IN ({yes}, {default}), m.`t`, NULL) AS `Transactions`, \
         IF(b.`SUPPORT` IN ({yes}, {default}), m.`x`, NULL) AS `XA`, \
         IF(b.`SUPPORT` IN ({yes}, {default}), m.`s`, NULL) AS `Savepoints` \
         FROM ({rows}) AS m LEFT JOIN information_schema.ENGINES AS b ON b.`ENGINE` = m.`e` \
         ORDER BY m.`n`",
        no = lit("NO"),
        yes = lit("YES"),
        default = lit("DEFAULT"),
    )
    .into_bytes()
}

/// `SHOW COLLATION [LIKE 'p' | WHERE e]` on 3306 as a query over the
/// MySQL-shaped `COLLATIONS` view ([`local_view`]), ordered by name as
/// MySQL lists them, with MySQL's seven columns. MariaDB's own `SHOW
/// COLLATION` also lists its 184 bare `uca1400_*` names with a `NULL` id,
/// which MySQL Connector/J reads as a number (`ResultSetTest.testPadding`;
/// gaps row 54). `None` when `text` isn't such a statement.
#[must_use]
pub fn show_collation_sql(text: &[u8], lit: Lit<'_>) -> Option<Vec<u8>> {
    let t = text.trim_ascii();
    let t = t.strip_suffix(b";").unwrap_or(t).trim_ascii_end();
    let word = |b: &[u8], w: &[u8]| {
        b.len() >= w.len()
            && b[..w.len()].eq_ignore_ascii_case(w)
            && b.get(w.len()).is_none_or(u8::is_ascii_whitespace)
    };
    if !word(t, b"SHOW") {
        return None;
    }
    let rest = t[4..].trim_ascii_start();
    if !word(rest, b"COLLATION") {
        return None;
    }
    let rest = rest[9..].trim_ascii_start();
    let filter: Vec<u8> = if rest.is_empty() {
        Vec::new()
    } else if word(rest, b"LIKE") {
        let mut f = b" WHERE `Collation` LIKE ".to_vec();
        f.extend_from_slice(rest[4..].trim_ascii_start());
        f
    } else if word(rest, b"WHERE") {
        let mut f = b" ".to_vec();
        f.extend_from_slice(rest);
        f
    } else {
        return None;
    };
    let view = local_view("COLLATIONS", Persona::MySql84, lit)?;
    let mut sql = format!(
        "SELECT * FROM (SELECT `COLLATION_NAME` AS `Collation`, `CHARACTER_SET_NAME` AS `Charset`, \
         `ID` AS `Id`, `IS_DEFAULT` AS `Default`, `IS_COMPILED` AS `Compiled`, `SORTLEN` AS `Sortlen`, \
         `PAD_ATTRIBUTE` AS `Pad_attribute` FROM ({view}) AS `_c`) AS `COLLATIONS`"
    )
    .into_bytes();
    sql.extend_from_slice(&filter);
    sql.extend_from_slice(b" ORDER BY `Collation`");
    Some(sql)
}

/// `SHOW INDEX` on 3306 rewritten over `STATISTICS` (see
/// [`show_index_sql`]); `None` leaves the statement to MariaDB.
#[must_use]
pub fn show_index_rewrite(
    info: &ShowInfo,
    text: &[u8],
    current: Option<&str>,
    map: &dyn NameMap,
    lit: Lit<'_>,
) -> Option<Vec<u8>> {
    let table = info.object.as_deref()?;
    let schema = target_schema(info.schema.as_deref(), current, map)?;
    let filter = info.filter.clone().map(|f| {
        let mut b = text.get(f).unwrap_or_default();
        while let Some((last, rest)) = b.split_last() {
            if *last == b';' || last.is_ascii_whitespace() {
                b = rest;
            } else {
                break;
            }
        }
        b
    });
    Some(show_index_sql(&schema, table, filter, lit))
}

/// The physical schema a `SHOW` or `DESCRIBE` reads: the one it names, or
/// the representative schema of the session's current database. `None`
/// for a system schema or when neither is known.
fn target_schema(named: Option<&str>, current: Option<&str>, map: &dyn NameMap) -> Option<String> {
    let s = match named {
        Some(s) => s.to_owned(),
        None => map
            .physical(current?, Purpose::Metadata)
            .ok()?
            .0
            .first()?
            .to_string(),
    };
    SystemSchema::parse(&s).is_none().then_some(s)
}

/// `SHOW [FULL] COLUMNS` on 3306 rewritten over the `COLUMNS` provider
/// (see [`show_columns_sql`]); `None` leaves the statement to MariaDB.
#[must_use]
pub fn show_columns_rewrite(
    info: &ShowInfo,
    text: &[u8],
    current: Option<&str>,
    map: &dyn NameMap,
    lit: Lit<'_>,
) -> Option<Vec<u8>> {
    let table = info.object.as_deref()?;
    let schema = target_schema(info.schema.as_deref(), current, map)?;
    let filter = info.filter.clone().map(|f| {
        let mut b = text.get(f).unwrap_or_default();
        while let Some((last, rest)) = b.split_last() {
            if *last == b';' || last.is_ascii_whitespace() {
                b = rest;
            } else {
                break;
            }
        }
        b
    });
    Some(show_columns_sql(
        &schema,
        table,
        info.full,
        info.like.as_deref(),
        filter,
        lit,
    ))
}

/// `DESCRIBE tbl [col]` on 3306 rewritten as `SHOW COLUMNS`; `None` leaves
/// the statement to MariaDB (`EXPLAIN` of a statement, odd shapes).
#[must_use]
pub fn describe_rewrite(
    text: &[u8],
    tokens: &[Token],
    nbe: bool,
    current: Option<&str>,
    map: &dyn NameMap,
    lit: Lit<'_>,
) -> Option<Vec<u8>> {
    let d = describe_target(text, tokens, nbe)?;
    let schema = target_schema(d.schema.as_deref(), current, map)?;
    Some(show_columns_sql(
        &schema,
        &d.table,
        false,
        d.like.as_deref(),
        None,
        lit,
    ))
}

/// Words after `EXPLAIN` that start a statement, not a table.
const NOT_TABLE: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "REPLACE",
    "WITH",
    "TABLE",
    "VALUES",
    "FORMAT",
    "EXTENDED",
    "PARTITIONS",
    "ANALYZE",
    "FOR",
];

/// What a `DESCRIBE` names: `DESC[RIBE] | EXPLAIN tbl [col | 'wild']`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTarget {
    /// The schema, if qualified (physical, after the name rewrite).
    pub schema: Option<String>,
    /// The table.
    pub table: String,
    /// The column name or pattern.
    pub like: Option<String>,
}

/// Reads a `DESCRIBE` of a table; `None` for `EXPLAIN` of a statement and
/// any other shape.
#[must_use]
pub fn describe_target(text: &[u8], tokens: &[Token], nbe: bool) -> Option<DescribeTarget> {
    let t: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize| -> Option<String> {
        t.get(j)
            .filter(|tk| tk.kind == TokenKind::Word)
            .map(|tk| String::from_utf8_lossy(tk.bytes(text)).to_ascii_uppercase())
    };
    let ident = |j: usize| -> Option<String> {
        t.get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&ident_value(tk.bytes(text))).into_owned())
    };
    let punct = |j: usize, c: u8| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && text[tk.start as usize] == c)
    };
    if !matches!(word(0)?.as_str(), "DESCRIBE" | "DESC" | "EXPLAIN") {
        return None;
    }

    if word(1).is_some_and(|w| NOT_TABLE.contains(&w.as_str())) {
        return None;
    }
    let first = ident(1)?;
    let (schema, table, mut j) = if punct(2, b'.') {
        (Some(first), ident(3)?, 4)
    } else {
        (None, first, 2)
    };
    let mut like = None;
    if let Some(tk) = t.get(j)
        && !punct(j, b';')
    {
        like = match tk.kind {
            TokenKind::String => {
                Some(String::from_utf8_lossy(&string_value(tk.bytes(text), nbe)?).into_owned())
            }
            TokenKind::Word | TokenKind::QuotedIdent => ident(j),
            _ => return None,
        };
        j += 1;
    }
    if t.get(j).is_some() && !punct(j, b';') {
        return None;
    }
    Some(DescribeTarget {
        schema,
        table,
        like,
    })
}

/// The index of the first `` `name` `` at the start of a column line
/// (two spaces then a backquoted name), and the name; `None` otherwise.
fn column_line_name(line: &str) -> Option<(String, usize)> {
    let rest = line.strip_prefix("  `")?;
    let mut name = String::new();
    let mut chars = rest.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '`' {
            if chars.peek().map(|&(_, n)| n) == Some('`') {
                name.push('`');
                chars.next();
                continue;
            }
            return Some((name, 3 + i + 1));
        }
        name.push(c);
    }
    None
}

/// The integer display width removed from the type at the start of `rest`
/// (`int(11) unsigned` → `int unsigned`), as MySQL 8.4 prints types.
fn strip_width(rest: &str) -> Option<String> {
    let lower = rest.to_ascii_lowercase();
    for ty in ["tinyint", "smallint", "mediumint", "int", "bigint", "year"] {
        let Some(after) = lower.strip_prefix(ty) else {
            continue;
        };
        let Some(digits) = after.strip_prefix('(') else {
            continue;
        };
        let n = digits.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 || !digits[n..].starts_with(')') {
            continue;
        }
        let tail = &rest[ty.len() + n + 2..];
        let tail_l = tail.to_ascii_lowercase();
        if (ty == "tinyint" && &digits[..n] == "1")
            || tail_l.starts_with(" zerofill")
            || tail_l.starts_with(" unsigned zerofill")
        {
            return None;
        }
        return Some(format!("{}{tail}", &rest[..ty.len()]));
    }
    None
}

/// Replaces MariaDB's `current_timestamp()` and `current_timestamp(n)`
/// with MySQL's spelling.
fn mysql_now(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("current_timestamp(") {
        out.push_str(&rest[..i]);
        let after = &rest[i + "current_timestamp(".len()..];
        let n = after.bytes().take_while(u8::is_ascii_digit).count();
        if after[n..].starts_with(')') {
            out.push_str("CURRENT_TIMESTAMP");
            if n > 0 {
                let _ = write!(out, "({})", &after[..n]);
            }
            rest = &after[n + 1..];
        } else {
            out.push_str("current_timestamp(");
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// Every MariaDB collation name in `s` with its MySQL name.
fn mysql_collations(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("utf8mb4_") {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        let n = tail
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        out.push_str(collation::mysql_name(&tail[..n]));
        rest = &tail[n..];
    }
    out.push_str(rest);
    out
}

/// One column line of `SHOW CREATE TABLE` in MySQL 8.4's spelling.
fn mysql_column_line(line: &str) -> String {
    let Some((name, at)) = column_line_name(line) else {
        return line.to_owned();
    };
    let head = &line[..at];
    let mut rest = line[at..].to_owned();
    let check = format!(" CHECK (json_valid({}))", quote(&name));
    if rest.starts_with(" longtext") && rest.contains(&check) {
        rest = rest.replacen(&check, "", 1);
        for prefix in [
            " longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            " longtext COLLATE utf8mb4_bin",
            " longtext CHARACTER SET utf8mb4",
            " longtext",
        ] {
            if let Some(r) = rest.strip_prefix(prefix) {
                rest = format!(" json{r}");
                break;
            }
        }
    } else if let Some(r) = rest.strip_prefix(' ')
        && let Some(stripped) = strip_width(r)
    {
        rest = format!(" {stripped}");
    }
    // An invisible column, as MySQL 8.4 writes it.
    let (body, comma) = match rest.strip_suffix(',') {
        Some(b) => (b.to_owned(), ","),
        None => (rest.clone(), ""),
    };
    if let Some(b) = body.strip_suffix(" INVISIBLE") {
        rest = format!("{b} /*!80023 INVISIBLE */{comma}");
    }
    format!("{head}{}", mysql_now(&rest))
}

/// `SHOW CREATE TABLE` text in MySQL 8.4's spelling, for 3306: JSON
/// columns as `json` without MariaDB's check, no integer display widths,
/// `CURRENT_TIMESTAMP`, MySQL collation names, invisible columns as
/// `/*!80023 INVISIBLE */` and `IGNORED` indexes as `/*!80000 INVISIBLE */`. Views and other text pass unchanged.
#[must_use]
pub fn mysql_create_table(text: &str) -> String {
    if !text.starts_with("CREATE TABLE") && !text.starts_with("CREATE TEMPORARY TABLE") {
        return text.to_owned();
    }
    // Generated columns' NOT NULL checks (R4 round 3, [`super::ddl`]): the
    // check line goes, the column says NOT NULL again.
    let mut not_null: Vec<String> = Vec::new();
    let kept: Vec<&str> = text
        .split('\n')
        .filter(|line| match nn_check_line(line) {
            Some(c) => {
                not_null.push(c);
                false
            }
            None => true,
        })
        .collect();
    let mut lines: Vec<String> = kept
        .iter()
        .map(|&line| {
            let t = line.trim_start();
            if line.starts_with("  `") {
                let l = mysql_column_line(line);
                match column_line_name(line) {
                    Some((name, _)) if not_null.contains(&name) => generated_not_null(&l),
                    _ => l,
                }
            } else if line.starts_with("  ")
                && (t.starts_with("KEY ")
                    || t.starts_with("UNIQUE KEY ")
                    || t.starts_with("FULLTEXT KEY ")
                    || t.starts_with("SPATIAL KEY "))
            {
                let (body, comma) = match line.strip_suffix(',') {
                    Some(b) => (b, ","),
                    None => (line, ""),
                };
                match body.strip_suffix(" IGNORED") {
                    Some(b) => format!("{b} /*!80000 INVISIBLE */{comma}"),
                    None => line.to_owned(),
                }
            } else {
                line.to_owned()
            }
        })
        .collect();
    if !not_null.is_empty() {
        // The element before the closing line loses a comma a removed
        // check line left behind.
        if let Some(close) = lines.iter().rposition(|l| l.starts_with(')'))
            && close > 0
            && let Some(prev) = lines.get_mut(close - 1)
            && prev.ends_with(',')
        {
            prev.pop();
        }
    }
    // R4 round 4: expressions printed the MySQL way (gaps row 62).
    super::exprprint::mysql_expressions(&mysql_collations(&lines.join("\n")))
}

/// The column of a ``  CONSTRAINT `_sprignn_<c>` CHECK (...)`` line.
fn nn_check_line(line: &str) -> Option<String> {
    let rest = line.strip_prefix("  CONSTRAINT ")?;
    let (name, _) = column_line_name(&format!("  {rest}"))?;
    super::ddl::nn_column(&name).map(str::to_owned)
}

/// A generated column's line with MySQL's `NOT NULL` after `VIRTUAL` or
/// `STORED`.
fn generated_not_null(line: &str) -> String {
    for kw in [" VIRTUAL", " STORED"] {
        if let Some(i) = line.rfind(kw) {
            let at = i + kw.len();
            return format!("{} NOT NULL{}", &line[..at], &line[at..]);
        }
    }
    line.to_owned()
}

#[cfg(test)]
mod tests;
