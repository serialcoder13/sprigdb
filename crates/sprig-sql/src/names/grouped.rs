//! `SHOW` lists of a database whose tables the advisor grouped (chapter 07,
//! "SHOW statements"; R1).
//!
//! The advisor moves a grouped table out of its database's first shard
//! into its group's schemas (chapter 16, "Regroup"), so MariaDB's own
//! `SHOW TABLES`, `SHOW TABLE STATUS` and `SHOW TRIGGERS`, which read one
//! schema, would leave it out. For such a database they become a `SELECT`
//! over the `information_schema` table they are built on, in the `SHOW`'s
//! shape and order, and that `information_schema` reference becomes the
//! usual inline view: one branch per representative schema
//! (`Purpose::Metadata`), so each table is listed once, with logical names
//! and internal objects hidden.
//!
//! ```text
//! SHOW FULL TABLES FROM shop LIKE 'o%'
//!   ─► SELECT * FROM (SELECT `TABLE_NAME` AS `Tables_in_shop`,
//!          IF(`TABLE_TYPE` = 'SYSTEM VERSIONED', 'BASE TABLE', `TABLE_TYPE`) AS `Table_type`
//!        FROM information_schema.TABLES
//!        WHERE `TABLE_SCHEMA` = 'shop' AND `TABLE_NAME` LIKE BINARY 'o%') AS `t`
//!      ORDER BY CAST(`Tables_in_shop` AS BINARY)
//! ```
//!
//! A `WHERE` filter is kept as the client wrote it, over the result's
//! columns, as MariaDB evaluates it. Databases with nothing grouped keep
//! MariaDB's own statements.

use sprig_common::names::quote;
use sprig_proto_mysql::packets::Persona;

use super::NameError;
use super::infoschema::{InfoSchemaCtx, rewrite_information_schema};
use super::map::NameMap;
use super::plan::{Action, PlanCtx};
use super::result::ResultMapper;
use super::rewrite::{StmtKind, SystemSchema, sql_string};
use super::show::{ShowInfo, ShowKind};
use crate::lex::{Token, TokenKind};

/// `SHOW TABLE STATUS`'s columns from `TABLES`: MariaDB's twenty, or
/// MySQL 8.4's first eighteen on 3306.
const TABLE_STATUS: &[(&str, &str)] = &[
    ("Name", "TABLE_NAME"),
    ("Engine", "ENGINE"),
    ("Version", "VERSION"),
    ("Row_format", "ROW_FORMAT"),
    ("Rows", "TABLE_ROWS"),
    ("Avg_row_length", "AVG_ROW_LENGTH"),
    ("Data_length", "DATA_LENGTH"),
    ("Max_data_length", "MAX_DATA_LENGTH"),
    ("Index_length", "INDEX_LENGTH"),
    ("Data_free", "DATA_FREE"),
    ("Auto_increment", "AUTO_INCREMENT"),
    ("Create_time", "CREATE_TIME"),
    ("Update_time", "UPDATE_TIME"),
    ("Check_time", "CHECK_TIME"),
    ("Collation", "TABLE_COLLATION"),
    ("Checksum", "CHECKSUM"),
    ("Create_options", "CREATE_OPTIONS"),
    ("Comment", "TABLE_COMMENT"),
    ("Max_index_length", "MAX_INDEX_LENGTH"),
    ("Temporary", "TEMPORARY"),
];

/// `SHOW TRIGGERS`'s columns from `TRIGGERS`.
const TRIGGERS: &[(&str, &str)] = &[
    ("Trigger", "TRIGGER_NAME"),
    ("Event", "EVENT_MANIPULATION"),
    ("Table", "EVENT_OBJECT_TABLE"),
    ("Statement", "ACTION_STATEMENT"),
    ("Timing", "ACTION_TIMING"),
    ("Created", "CREATED"),
    ("sql_mode", "SQL_MODE"),
    ("Definer", "DEFINER"),
    ("character_set_client", "CHARACTER_SET_CLIENT"),
    ("collation_connection", "COLLATION_CONNECTION"),
    ("Database Collation", "DATABASE_COLLATION"),
];

/// The raw text of the string after `LIKE` in `tokens` (a `SHOW`'s
/// pattern), quotes and escapes as the client wrote them.
fn like_literal<'s>(text: &'s [u8], tokens: &[Token]) -> Option<&'s [u8]> {
    let t: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let i = t.iter().position(|tk| {
        tk.kind == TokenKind::Word && tk.bytes(text).eq_ignore_ascii_case(b"LIKE")
    })?;
    let s = t.get(i + 1)?;
    matches!(s.kind, TokenKind::String | TokenKind::NationalString).then(|| s.bytes(text))
}

/// The client's `WHERE ...`, without a trailing `;`.
fn filter_text<'s>(text: &'s [u8], info: &ShowInfo) -> Option<&'s [u8]> {
    let mut b = text.get(info.filter.clone()?)?;
    while let Some((last, rest)) = b.split_last() {
        if *last == b';' || last.is_ascii_whitespace() {
            b = rest;
        } else {
            break;
        }
    }
    Some(b)
}

/// `SHOW [FULL] TABLES`, `SHOW TABLE STATUS` or `SHOW TRIGGERS` of a
/// database with grouped tables, as a `SELECT` over `information_schema`
/// (see the module docs); `None` for other statements and databases.
///
/// # Errors
///
/// The inline view's ([`rewrite_information_schema`]).
pub(super) fn listing(
    info: &ShowInfo,
    text: &[u8],
    tokens: &[Token],
    current: Option<&str>,
    ctx: &PlanCtx<'_>,
) -> Result<Option<Action>, NameError> {
    if !matches!(
        info.kind,
        ShowKind::Tables | ShowKind::TableStatus | ShowKind::Triggers
    ) {
        return Ok(None);
    }
    let map: &dyn NameMap = ctx.rewrite.map;
    let db = match info.schema.as_deref() {
        Some(s) if SystemSchema::parse(s).is_some() => return Ok(None),
        Some(s) => map.logical(s),
        None => current
            .filter(|c| SystemSchema::parse(c).is_none())
            .and_then(|c| map.database(c)),
    };
    let Some(db) = db else { return Ok(None) };
    if map.grouped_schemas(db.id).is_empty() {
        return Ok(None);
    }
    let nbe = ctx.rewrite.lex.no_backslash_escapes;
    let lit = |v: &str| sql_string(v, nbe);
    let logical = db.name.clone();
    let like = like_literal(text, tokens).map(|b| String::from_utf8_lossy(b).into_owned());
    let mut cols: Vec<String> = Vec::new();
    let (table, schema_col, name_col, order) = match info.kind {
        ShowKind::Tables => {
            let head = quote(&format!("Tables_in_{logical}"));
            cols.push(format!("`TABLE_NAME` AS {head}"));
            if info.full {
                cols.push(format!(
                    "IF(`TABLE_TYPE` = {}, {}, `TABLE_TYPE`) AS `Table_type`",
                    lit("SYSTEM VERSIONED"),
                    lit("BASE TABLE")
                ));
            }
            let order = format!("CAST({head} AS BINARY)");
            ("TABLES", "TABLE_SCHEMA", "TABLE_NAME", order)
        }
        ShowKind::TableStatus => {
            let n = if ctx.rewrite.lex.persona == Persona::MySql84 {
                18
            } else {
                TABLE_STATUS.len()
            };
            for (out, src) in &TABLE_STATUS[..n] {
                cols.push(format!("{} AS {}", quote(src), quote(out)));
            }
            (
                "TABLES",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "CAST(`Name` AS BINARY)".to_owned(),
            )
        }
        _ => {
            for (out, src) in TRIGGERS {
                cols.push(format!("{} AS {}", quote(src), quote(out)));
            }
            cols.push("`ACTION_ORDER` AS `__sprig_order`".to_owned());
            (
                "TRIGGERS",
                "EVENT_OBJECT_SCHEMA",
                "EVENT_OBJECT_TABLE",
                "CAST(`Table` AS BINARY), `Event`, `Timing`, `__sprig_order`".to_owned(),
            )
        }
    };
    let mut inner = format!(
        "SELECT {} FROM information_schema.{table} WHERE {} = {}",
        cols.join(", "),
        quote(schema_col),
        lit(&logical)
    );
    if let Some(p) = &like {
        inner.push_str(&format!(" AND {} LIKE BINARY {p}", quote(name_col)));
    }
    let outer_cols = if info.kind == ShowKind::Triggers {
        TRIGGERS
            .iter()
            .map(|(c, _)| quote(c))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        "*".to_owned()
    };
    let mut sql = format!("SELECT {outer_cols} FROM ({inner}) AS `t`").into_bytes();
    if let Some(f) = filter_text(text, info) {
        sql.push(b' ');
        sql.extend_from_slice(f);
    }
    sql.extend_from_slice(format!(" ORDER BY {order}").as_bytes());
    let is_ctx = InfoSchemaCtx {
        map,
        lex: ctx.rewrite.lex,
        vis: ctx.vis,
        registry: ctx.registry,
        sessions: ctx.sessions,
        visible: ctx.visible,
        in_information_schema: false,
        user: ctx.rewrite.account.map(|a| (a.user, a.client_host)),
        stored_names: ctx.stored_names,
    };
    let Some(r) = rewrite_information_schema(&sql, StmtKind::Select, &is_ctx)? else {
        return Ok(None);
    };
    Ok(Some(Action::Forward {
        sql: r.sql,
        mapper: ResultMapper::scrub(),
    }))
}

#[cfg(test)]
mod tests;
