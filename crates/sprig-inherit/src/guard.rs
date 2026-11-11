//! Schema changes while an object is shared (chapter 42; Kiran's decision
//! N7 of 8 November 2026): while any share of an inheritable object exists,
//! only additive changes are allowed; destructive ones fail with 39895,
//! naming the shares that block them.
//!
//! | Allowed while shared | Refused while shared |
//! |---|---|
//! | `ADD COLUMN` nullable or with a default; `ADD INDEX`; `DROP INDEX` | `DROP TABLE`, `TRUNCATE`, `RENAME TABLE`, `DROP DATABASE` holding it |
//! | widening a type (`INT` → `BIGINT`, longer `VARCHAR`, `ENUM` / `SET` members at the end, wider `DECIMAL`, `FLOAT` → `DOUBLE`, `NOT NULL` → `NULL`) | `DROP COLUMN`, `RENAME COLUMN`, narrowing or changing a type, a charset or collation |
//! | comments, defaults, column order | `NOT NULL` without a default, `UNIQUE`, `CHECK`, foreign and primary keys, `DROP SPRIG INHERITABLE` |
//!
//! Anything this module can't classify (a statement sqlparser-rs can't
//! read, an operation not in the table) counts as destructive. The gateway
//! refuses early, from its snapshot; [`SharedCheck`] refuses again in
//! chapter 21's prepare phase, under the catalog lock, from the catalog
//! itself, so a share created by another session in between is seen
//! (decision I7). `DROP TABLE`, `RENAME TABLE`, `TRUNCATE` and `DROP
//! DATABASE` are destructive by kind; an `ALTER TABLE` is marked
//! destructive by this module ([`DESTRUCTIVE`]) when it classifies it,
//! because the engine can't read column types.
//!
//! The statements that pass run here too, so the object's column list
//! and field slots follow the change.

use sprig_common::{Result, SprigCode, SprigError};
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnOption, DataType, Statement, TableConstraint,
};

use std::sync::Arc;

use sprig_catalog::ddl::{
    DdlRequest, DdlStatement, DdlTx, ObjectName, PrepareCheck, database_key, object_key,
};
use sprig_catalog::store::{Namespace, ObjectKind};

use crate::grammar::Name;
use crate::model::InheritableObject;
use crate::plan::{Ctx, Resolved};
use crate::rewrite::parse_one;
use crate::store::InheritChange;
use crate::view::q;
use crate::{Handled, Runner};

tokio::task_local! {
    /// Why the `ALTER TABLE` this task is running is destructive: the
    /// gateway's classification, for [`SharedCheck`].
    pub static DESTRUCTIVE: String;
}

/// Chapter 42's prepare-phase check (see the module docs), registered on
/// the gateway's DDL engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct SharedCheck;

impl SharedCheck {
    /// The check as the engine takes it.
    #[must_use]
    pub fn shared() -> Arc<dyn PrepareCheck> {
        Arc::new(Self)
    }
}

async fn object_of(
    tx: &mut dyn DdlTx,
    req: &DdlRequest<'_>,
    n: &ObjectName,
) -> sprig_common::Result<Option<u64>> {
    let Some(db) = n.db.as_deref().or(req.current_db) else {
        return Ok(None);
    };
    let lctn = req.lower_case_table_names;
    let Some(d) = tx.database_by_name(&database_key(db, lctn)).await? else {
        return Ok(None);
    };
    let o = tx
        .object_by_name(
            d.db_id,
            Namespace::Relation,
            &object_key(ObjectKind::Table, &n.name, lctn),
        )
        .await?;
    Ok(o.map(|o| o.object_id.0))
}

async fn shares_of(tx: &mut dyn DdlTx, objects: &[u64]) -> sprig_common::Result<Vec<String>> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    let list = objects
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let rows = tx
        .query_rows(&format!(
            "SELECT s.name FROM {{catalog}}.shares s JOIN {{catalog}}.inheritable_objects i \
             ON i.object_id = s.object_id WHERE s.object_id IN ({list}) AND s.role = 'granted' \
             AND s.state = 'active' ORDER BY s.share_id"
        ))
        .await?
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect())
}

#[async_trait::async_trait]
impl PrepareCheck for SharedCheck {
    async fn check(&self, req: &DdlRequest<'_>, tx: &mut dyn DdlTx) -> sprig_common::Result<()> {
        let (what, names): (String, Vec<&ObjectName>) = match &req.statement {
            DdlStatement::DropObjects {
                kind: ObjectKind::Table,
                names,
                ..
            } => ("DROP TABLE".into(), names.iter().collect()),
            DdlStatement::RenameTables { pairs, .. } => (
                "RENAME TABLE".into(),
                pairs.iter().map(|(a, _)| a).collect(),
            ),
            DdlStatement::Truncate { name } => ("TRUNCATE".into(), vec![name]),
            DdlStatement::AlterObject {
                kind: ObjectKind::Table,
                name,
                ..
            } => match DESTRUCTIVE.try_with(Clone::clone) {
                Ok(why) => (format!("ALTER TABLE ({why})"), vec![name]),
                Err(_) => return Ok(()),
            },
            DdlStatement::DropDatabase { name, .. } => {
                let Some(d) = tx
                    .database_by_name(&database_key(name, req.lower_case_table_names))
                    .await?
                else {
                    return Ok(());
                };
                let rows = tx
                    .query_rows(&format!(
                        "SELECT s.name FROM {{catalog}}.shares s JOIN {{catalog}}.objects o ON \
                         o.object_id = s.object_id WHERE o.db_id = {} AND s.role = 'granted' \
                         AND s.state = 'active' ORDER BY s.share_id",
                        d.db_id.0
                    ))
                    .await?
                    .unwrap_or_default();
                let names: Vec<String> = rows
                    .into_iter()
                    .filter_map(|r| r.into_iter().next().flatten())
                    .collect();
                return refusal("DROP DATABASE", name, &names);
            }
            _ => return Ok(()),
        };
        for n in names {
            if let Some(o) = object_of(tx, req, n).await? {
                refusal(&what, &n.name, &shares_of(tx, &[o]).await?)?;
            }
        }
        Ok(())
    }
}

fn refusal(what: &str, on: &str, shares: &[String]) -> sprig_common::Result<()> {
    if shares.is_empty() {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::InheritedObjectInUse,
        format!(
            "{what} on {on} is refused while it is shared ({}): drop the shares first",
            shares.join(", ")
        ),
    ))
}

/// 39895 when `o` is shared.
///
/// # Errors
/// 39895.
pub fn refuse_if_shared(ctx: &Ctx<'_>, o: &InheritableObject, what: &str) -> Result<()> {
    let names: Vec<String> = ctx
        .state
        .shares_of(o.object_id)
        .map(|s| s.name.clone())
        .collect();
    if names.is_empty() {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::InheritedObjectInUse,
        format!(
            "{what} on {} is refused while it is shared ({}): drop the shares first",
            o.table,
            names.join(", ")
        ),
    ))
}

/// 39897 when this tenant receives `o` from its parent tenant: the
/// parent's share decides that the table is here (a child may add to it,
/// not remove or reshape what it inherits).
///
/// # Errors
/// 39897.
pub fn refuse_if_received(ctx: &Ctx<'_>, o: &InheritableObject, what: &str) -> Result<()> {
    if ctx.up_edge(o)?.is_none() {
        return Ok(());
    }
    let parent = ctx
        .cat
        .ancestry
        .last()
        .map_or_else(String::new, |a| a.name.clone());
    Err(SprigError::new(
        SprigCode::InheritNotOverridable,
        format!(
            "{what} on {} is refused: this tenant inherits it from '{parent}' (a child may add \
             columns and tables, not remove or change inherited ones)",
            o.table
        ),
    ))
}

/// [`refuse_if_shared`], reading the catalog itself (as the provisioning
/// account) rather than the snapshot: for statements no DDL prepare phase
/// sees (`TRUNCATE` of an inheritable object runs as a `DELETE`), so a
/// share made in between by another session is seen.
///
/// # Errors
/// 39895; the catalog's.
pub async fn refuse_if_shared_now(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    o: &InheritableObject,
    what: &str,
) -> Result<()> {
    refuse_if_shared(ctx, o, what)?;
    let t = ctx.cat.tenant;
    let rows = match r
        .ctl_query(
            t,
            &format!(
                "SELECT name FROM `t{t}_sprig`.shares WHERE object_id = {} AND role = 'granted' \
                 AND state = 'active' ORDER BY share_id",
                o.object_id.0
            ),
        )
        .await
    {
        Ok(rows) => rows,
        // A runner without provisioning access: the snapshot's answer.
        Err(_) => return Ok(()),
    };
    let names: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .collect();
    if names.is_empty() {
        return Ok(());
    }
    refusal(what, &o.table, &names)
}

/// `DROP TABLE`: refused for a shared object, and for one this tenant
/// receives from its parent.
///
/// # Errors
/// 39895; 39897.
pub fn drop_tables(names: &[Name], ctx: &Ctx<'_>) -> Result<()> {
    for n in names {
        if let Some(r) = ctx.resolve(n)? {
            refuse_if_shared(ctx, &r.obj, "DROP TABLE")?;
            refuse_if_received(ctx, &r.obj, "DROP TABLE")?;
        }
    }
    Ok(())
}

/// `RENAME TABLE`: refused for a shared object, and for one this tenant
/// receives from its parent.
///
/// # Errors
/// 39895; 39897.
pub fn rename(names: &[Name], ctx: &Ctx<'_>) -> Result<()> {
    for n in names {
        if let Some(r) = ctx.resolve(n)? {
            refuse_if_shared(ctx, &r.obj, "RENAME TABLE")?;
            refuse_if_received(ctx, &r.obj, "RENAME TABLE")?;
        }
    }
    Ok(())
}

/// `DROP DATABASE`: refused when it holds a shared object.
///
/// # Errors
/// 39895.
pub fn drop_database(db: &str, ctx: &Ctx<'_>) -> Result<()> {
    let Some((id, _)) = ctx.db_of(&Name {
        db: Some(db.to_owned()),
        name: String::new(),
    }) else {
        return Ok(());
    };
    for o in ctx.state.objects().filter(|o| o.db == id) {
        refuse_if_shared(ctx, o, "DROP DATABASE")?;
        refuse_if_received(ctx, o, "DROP DATABASE")?;
    }
    Ok(())
}

/// A column as `SHOW COLUMNS` describes it.
#[derive(Debug, Clone)]
pub(crate) struct OldColumn {
    pub(crate) name: String,
    pub(crate) ty: String,
    pub(crate) nullable: bool,
    /// `Extra` (`auto_increment`, ...).
    pub(crate) extra: String,
}

/// The `AUTO_INCREMENT` column among `cols`.
pub(crate) fn auto_column(cols: &[OldColumn]) -> Option<String> {
    cols.iter()
        .find(|c| c.extra.to_ascii_lowercase().contains("auto_increment"))
        .map(|c| c.name.clone())
}

pub(crate) async fn show_columns(r: &mut dyn Runner, layer: &str) -> Result<Vec<OldColumn>> {
    let rows = r.query(&format!("SHOW COLUMNS FROM {layer}")).await?;
    let text = |row: &Vec<Option<Vec<u8>>>, i: usize| {
        row.get(i)
            .and_then(Option::as_ref)
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default()
    };
    Ok(rows
        .rows
        .iter()
        .map(|row| OldColumn {
            name: text(row, 0),
            ty: text(row, 1),
            nullable: text(row, 2).eq_ignore_ascii_case("YES"),
            extra: text(row, 5),
        })
        .collect())
}

/// User `UNIQUE` keys as `SHOW INDEX` lists them (the `_sprig_space` part
/// is hidden by chapter 07).
pub(crate) async fn show_uniques(
    r: &mut dyn Runner,
    layer: &str,
) -> Result<(Vec<String>, Vec<crate::model::UniqueKey>)> {
    let rows = r.query(&format!("SHOW INDEX FROM {layer}")).await?;
    let text = |row: &Vec<Option<Vec<u8>>>, i: usize| {
        row.get(i)
            .and_then(Option::as_ref)
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default()
    };
    let mut pk: Vec<(u32, String)> = Vec::new();
    let mut uniques: Vec<crate::model::UniqueKey> = Vec::new();
    let mut seq: Vec<(String, u32, String)> = Vec::new();
    for row in &rows.rows {
        let non_unique = text(row, 1);
        let key = text(row, 2);
        let n: u32 = text(row, 3).parse().unwrap_or(0);
        let col = text(row, 4);
        if non_unique != "0" || col.to_ascii_lowercase().starts_with("_sprig_") {
            continue;
        }
        if key == "PRIMARY" {
            pk.push((n, col));
        } else {
            seq.push((key, n, col));
        }
    }
    pk.sort();
    seq.sort();
    for (key, _, col) in seq {
        match uniques.iter_mut().find(|u| u.name == key) {
            Some(u) => u.columns.push(col),
            None => uniques.push(crate::model::UniqueKey {
                name: key,
                columns: vec![col],
            }),
        }
    }
    Ok((pk.into_iter().map(|(_, c)| c).collect(), uniques))
}

fn norm(t: &str) -> String {
    let t = t.to_ascii_lowercase().replace(", ", ",");
    // Display widths of integers don't matter.
    let ints = [
        "tinyint",
        "smallint",
        "mediumint",
        "bigint",
        "int",
        "integer",
    ];
    for i in ints {
        if let Some(rest) = t.strip_prefix(i)
            && rest.starts_with('(')
            && let Some(close) = rest.find(')')
        {
            return format!("{i}{}", &rest[close + 1..]).trim().to_owned();
        }
    }
    t.trim().to_owned()
}

fn int_rank(base: &str) -> Option<u8> {
    match base {
        "tinyint" => Some(1),
        "smallint" => Some(2),
        "mediumint" => Some(3),
        "int" | "integer" => Some(4),
        "bigint" => Some(5),
        _ => None,
    }
}

fn args(t: &str) -> Option<(String, Vec<String>, String)> {
    let open = t.find('(')?;
    let close = t.rfind(')')?;
    let base = t[..open].trim().to_owned();
    let inner = &t[open + 1..close];
    let rest = t[close + 1..].trim().to_owned();
    // ENUM members may contain commas inside quotes.
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in inner.chars() {
        match ch {
            '\'' => {
                quoted = !quoted;
                cur.push(ch);
            }
            ',' if !quoted => parts.push(std::mem::take(&mut cur).trim().to_owned()),
            _ => cur.push(ch),
        }
    }
    parts.push(cur.trim().to_owned());
    Some((base, parts, rest))
}

/// Whether type `new` holds every value of type `old` unchanged.
#[must_use]
pub fn widens(old: &str, new: &str) -> bool {
    let (o, n) = (norm(old), norm(new));
    if o == n {
        return true;
    }
    let split = |t: &str| {
        let unsigned = t.contains("unsigned");
        let base = t.split(['(', ' ']).next().unwrap_or("").to_owned();
        (base, unsigned)
    };
    let ((ob, ou), (nb, nu)) = (split(&o), split(&n));
    if let (Some(a), Some(b)) = (int_rank(&ob), int_rank(&nb)) {
        return ou == nu && b >= a;
    }
    let texts = ["tinytext", "text", "mediumtext", "longtext"];
    let trank = |b: &str| texts.iter().position(|x| *x == b);
    match (ob.as_str(), nb.as_str()) {
        ("varchar" | "char", "varchar") | ("char", "char") => {
            let (Some((_, oa, _)), Some((_, na, _))) = (args(&o), args(&n)) else {
                return false;
            };
            let len = |v: &[String]| v.first().and_then(|x| x.parse::<u64>().ok());
            matches!((len(&oa), len(&na)), (Some(a), Some(b)) if b >= a)
                && (ob == nb || ob == "char")
        }
        ("varchar" | "char", t) if trank(t).is_some() => true,
        (a, b) if trank(a).is_some() && trank(b).is_some() => trank(b) >= trank(a),
        ("decimal" | "numeric", "decimal" | "numeric") => {
            let (Some((_, oa, ors)), Some((_, na, nrs))) = (args(&o), args(&n)) else {
                return false;
            };
            let num =
                |v: &[String], i: usize| v.get(i).and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
            let (op, os, np, ns) = (num(&oa, 0), num(&oa, 1), num(&na, 0), num(&na, 1));
            ors == nrs && ns >= os && np - ns >= op - os
        }
        ("float", "double") => ou == nu,
        ("enum", "enum") | ("set", "set") => {
            let (Some((_, oa, _)), Some((_, na, _))) = (args(&o), args(&n)) else {
                return false;
            };
            na.len() >= oa.len() && na[..oa.len()] == oa[..]
        }
        _ => false,
    }
}

fn type_and_nulls(dt: &DataType, opts: &[ColumnOption]) -> (String, Option<bool>, bool, bool) {
    let mut nullable = None;
    let mut has_default = false;
    let mut charset = false;
    let mut ty = dt.to_string();
    for o in opts {
        match o {
            ColumnOption::Null => nullable = Some(true),
            ColumnOption::NotNull => nullable = Some(false),
            ColumnOption::Default(_) => has_default = true,
            ColumnOption::CharacterSet(_) | ColumnOption::Collation(_) => charset = true,
            ColumnOption::DialectSpecific(t) => {
                let s = t
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_ascii_uppercase();
                if s.contains("UNSIGNED") && !ty.to_ascii_uppercase().contains("UNSIGNED") {
                    ty.push_str(" UNSIGNED");
                }
            }
            _ => {}
        }
    }
    (ty, nullable, has_default, charset)
}

/// Why an operation is destructive, if it is.
fn destructive(op: &AlterTableOperation, old: &[OldColumn]) -> Option<String> {
    let find = |n: &str| old.iter().find(|c| c.name.eq_ignore_ascii_case(n));
    let changed = |name: &str, dt: &DataType, opts: &[ColumnOption]| -> Option<String> {
        let (ty, nullable, _, charset) = type_and_nulls(dt, opts);
        let Some(c) = find(name) else {
            return Some(format!("an unknown column {name}"));
        };
        if charset {
            return Some(format!("a charset or collation change of {name}"));
        }
        if !widens(&c.ty, &ty) {
            return Some(format!("changing {name} from {} to {ty}", c.ty));
        }
        // Without NULL / NOT NULL, MODIFY makes the column nullable.
        if c.nullable && nullable == Some(false) {
            return Some(format!("NOT NULL on {name}"));
        }
        None
    };
    match op {
        AlterTableOperation::AddColumn { column_def, .. } => {
            let opts: Vec<ColumnOption> = column_def
                .options
                .iter()
                .map(|o| o.option.clone())
                .collect();
            let (_, nullable, has_default, _) = type_and_nulls(&column_def.data_type, &opts);
            if nullable == Some(false) && !has_default {
                return Some(format!(
                    "adding {} NOT NULL without a default",
                    column_def.name.value
                ));
            }
            if opts.iter().any(|o| {
                matches!(
                    o,
                    ColumnOption::Unique(_)
                        | ColumnOption::PrimaryKey(_)
                        | ColumnOption::ForeignKey(_)
                        | ColumnOption::Check(_)
                )
            }) {
                return Some(format!(
                    "a constraint on new column {}",
                    column_def.name.value
                ));
            }
            None
        }
        AlterTableOperation::AddConstraint { constraint, .. } => match constraint {
            TableConstraint::Unique(_)
            | TableConstraint::PrimaryKey(_)
            | TableConstraint::ForeignKey(_)
            | TableConstraint::Check(_) => Some("a UNIQUE, CHECK, primary or foreign key".into()),
            _ => None,
        },
        AlterTableOperation::DropIndex { name } => {
            if name.value.eq_ignore_ascii_case("PRIMARY") {
                Some("dropping the primary key".into())
            } else {
                None
            }
        }
        AlterTableOperation::DropConstraint { .. } | AlterTableOperation::DropForeignKey { .. } => {
            None
        }
        AlterTableOperation::ModifyColumn {
            col_name,
            data_type,
            options,
            ..
        } => changed(&col_name.value, data_type, options),
        AlterTableOperation::ChangeColumn {
            old_name,
            new_name,
            data_type,
            options,
            ..
        } => {
            if !old_name.value.eq_ignore_ascii_case(&new_name.value) {
                return Some(format!("renaming {}", old_name.value));
            }
            changed(&old_name.value, data_type, options)
        }
        AlterTableOperation::AlterColumn { column_name, op } => match op {
            AlterColumnOperation::SetDefault { .. }
            | AlterColumnOperation::DropDefault
            | AlterColumnOperation::DropNotNull => None,
            AlterColumnOperation::SetDataType { data_type, .. } => {
                changed(&column_name.value, data_type, &[ColumnOption::Null])
            }
            _ => Some(format!("altering {}", column_name.value)),
        },
        AlterTableOperation::Algorithm { .. } | AlterTableOperation::Lock { .. } => None,
        AlterTableOperation::DropColumn { .. } => Some("dropping a column".into()),
        AlterTableOperation::RenameColumn { .. } => Some("renaming a column".into()),
        AlterTableOperation::RenameTable { .. } => Some("renaming the table".into()),
        AlterTableOperation::DropPrimaryKey { .. } => Some("dropping the primary key".into()),
        other => Some(format!("`{other}`")),
    }
}

/// `ALTER TABLE` of an inheritable object: refused when destructive and
/// shared; otherwise run, and the object's columns, slots and unique keys
/// follow.
///
/// # Errors
/// 39895; the statement's.
pub async fn alter(sql: &str, n: &Name, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let Some(res) = ctx.resolve(n)? else {
        return Ok(Handled::NotInherit);
    };
    // Classified always: the prepare phase refuses a destructive change
    // if a share exists by then, even one this snapshot doesn't show.
    let old = show_columns(r, &res.layer).await?;
    let why = match parse_one(sql) {
        Ok(Statement::AlterTable(at)) => at.operations.iter().find_map(|op| destructive(op, &old)),
        _ => Some("a change SprigDB can't classify".into()),
    };
    if let Some(why) = &why
        && ctx.state.shares_of(res.obj.object_id).next().is_some()
    {
        return Err(
            refuse_if_shared(ctx, &res.obj, &format!("ALTER TABLE ({why})"))
                .err()
                .unwrap_or_else(|| SprigError::internal("shared but no shares")),
        );
    }
    if let Some(why) = &why {
        refuse_if_received(ctx, &res.obj, &format!("ALTER TABLE ({why})"))?;
    }
    // A column a descendant tenant already has would come down to it
    // (question Q3; the default: refuse the parent's change).
    if let Ok(Statement::AlterTable(at)) = parse_one(sql) {
        for op in &at.operations {
            if let AlterTableOperation::AddColumn { column_def, .. } = op {
                crate::links::refuse_clash(ctx, r, &res, Some(&column_def.name.value)).await?;
            }
        }
    }
    let d = match why {
        Some(why) => DESTRUCTIVE.scope(why, r.exec(sql)).await?,
        None => r.exec(sql).await?,
    };
    follow_columns(ctx, r, &res).await?;
    Ok(Handled::Ok(d))
}

/// Reads the object's columns and unique keys again after a change and
/// commits them (new columns get new slots; new unique keys are widened
/// with the space).
pub(crate) async fn follow_columns(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
) -> Result<()> {
    let cols: Vec<String> = show_columns(r, &res.layer)
        .await?
        .into_iter()
        .map(|c| c.name)
        .collect();
    let (_, uniques) = show_uniques(r, &res.layer).await?;
    let mut o = (*res.obj).clone();
    let mut changed = false;
    if let Some(n) = o.with_slots_for(&cols) {
        o = n;
        changed = true;
    }
    if o.auto.is_none() {
        let old = show_columns(r, &res.layer).await?;
        if let Some(a) = auto_column(&old) {
            o.auto = Some(a);
            changed = true;
        }
    }
    for u in &uniques {
        if !res.obj.uniques.iter().any(|x| x.name == u.name) {
            // A new user UNIQUE key: unique per layer.
            r.exec(&format!(
                "ALTER TABLE {} DROP INDEX {k}, ADD UNIQUE KEY {k} ({}, `_sprig_space`)",
                res.layer,
                u.columns
                    .iter()
                    .map(|c| q(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                k = q(&u.name)
            ))
            .await?;
        }
    }
    if uniques != o.uniques {
        o.uniques = uniques;
        changed = true;
    }
    if changed {
        r.commit(&InheritChange {
            upsert_objects: vec![o],
            ..InheritChange::default()
        })
        .await?;
    }
    let _ = ctx;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widening() {
        assert!(widens("int(11)", "BIGINT"));
        assert!(!widens("bigint(20)", "INT"));
        assert!(!widens("int(11) unsigned", "BIGINT"));
        assert!(widens("varchar(32)", "VARCHAR(64)"));
        assert!(!widens("varchar(64)", "VARCHAR(32)"));
        assert!(widens("varchar(64)", "TEXT"));
        assert!(widens("decimal(10,2)", "DECIMAL(12,2)"));
        assert!(!widens("decimal(10,2)", "DECIMAL(10,3)"));
        assert!(widens("decimal(10,2)", "DECIMAL(11,3)"));
        assert!(widens("enum('a','b')", "ENUM('a', 'b', 'c')"));
        assert!(!widens("enum('a','b')", "ENUM('b', 'a', 'c')"));
        assert!(!widens("varchar(10)", "INT"));
    }

    fn ops(sql: &str) -> Vec<AlterTableOperation> {
        match parse_one(sql).unwrap() {
            Statement::AlterTable(a) => a.operations,
            _ => panic!(),
        }
    }

    #[test]
    fn classification() {
        let old = vec![
            OldColumn {
                name: "name".into(),
                ty: "varchar(200)".into(),
                nullable: false,
                extra: String::new(),
            },
            OldColumn {
                name: "price".into(),
                ty: "decimal(10,2)".into(),
                nullable: true,
                extra: String::new(),
            },
        ];
        let d = |sql: &str| ops(sql).iter().find_map(|o| destructive(o, &old));
        assert_eq!(d("ALTER TABLE p ADD COLUMN sku INT NULL"), None);
        assert_eq!(
            d("ALTER TABLE p ADD COLUMN sku INT NOT NULL DEFAULT 0"),
            None
        );
        assert!(d("ALTER TABLE p ADD COLUMN sku INT NOT NULL").is_some());
        assert_eq!(d("ALTER TABLE p ADD INDEX k (name)"), None);
        assert!(d("ALTER TABLE p ADD UNIQUE KEY k (name)").is_some());
        assert!(d("ALTER TABLE p DROP COLUMN price").is_some());
        assert_eq!(d("ALTER TABLE p MODIFY name VARCHAR(300) NOT NULL"), None);
        assert!(d("ALTER TABLE p MODIFY name VARCHAR(100) NOT NULL").is_some());
        assert!(d("ALTER TABLE p MODIFY price DECIMAL(10,2) NOT NULL").is_some());
        assert!(d("ALTER TABLE p CHANGE name title VARCHAR(200)").is_some());
        assert_eq!(d("ALTER TABLE p ALTER COLUMN price SET DEFAULT 1"), None);
        assert!(d("ALTER TABLE p RENAME TO q").is_some());
    }
}
