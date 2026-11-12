//! Spaces, home spaces, shares and `SPRIG INHERITABLE` (chapter 42,
//! "Interfaces" and "The physical layer"; build steps 1-3).
//!
//! `ALTER TABLE t SPRIG INHERITABLE KEY (k)` adds the invisible columns
//! `_sprig_space`, `_sprig_state` and `_sprig_ovr`, widens the primary
//! key and every user `UNIQUE` key with `_sprig_space`, and adds the
//! `_sprig_layer` index, in one `ALTER TABLE` (MariaDB copies the table;
//! the chapter's copy-and-swap job is not used). Existing rows become the
//! root space's own rows. The key must be the table's primary key.

use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::Done;
use sqlparser::ast::{Expr, Visit, Visitor};

use crate::Runner;
use crate::grammar::{Name, ShareChange, To, unsupported};
use crate::model::{
    ALL_BITS, InheritableObject, MergeMode, ROOT_SPACE, Share, ShareTarget, SpaceId, Take,
};
use crate::plan::{Ctx, not_found, state_of};
use crate::rewrite::parse_expr;
use crate::store::{InheritChange, NewSpace};
use crate::view::{Top, q};

fn need_space_admin(ctx: &Ctx<'_>) -> Result<()> {
    if ctx.privileges.space_admin() {
        Ok(())
    } else {
        Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the CREATE USER or SPRIG_TENANT_ADMIN \
             privilege(s) for this operation",
        ))
    }
}

fn denied(what: &str) -> SprigError {
    SprigError::new(SprigCode::SpaceAccessDenied, what.to_owned())
}

pub(crate) fn no_table(name: &Name, ctx: &Ctx<'_>) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!(
            "Table '{}.{}' doesn't exist",
            name.db
                .clone()
                .or_else(|| ctx.current_db.clone())
                .unwrap_or_default(),
            name.name
        ),
    )
}

pub(crate) fn root_change(ctx: &Ctx<'_>) -> InheritChange {
    InheritChange {
        root_name: Some(ctx.tenant_name.to_owned()),
        ..InheritChange::default()
    }
}

/// `USE SPACE s` (`None`: the user's home space).
///
/// # Errors
/// 39880 for an unknown space; 39884 outside the user's home subtree.
pub fn use_space(ctx: &Ctx<'_>, name: Option<&str>) -> Result<SpaceId> {
    let home = ctx.user.map_or(ROOT_SPACE, |u| ctx.state.home(u));
    let Some(name) = name else {
        return Ok(home);
    };
    let s = ctx
        .state
        .space_named(name, ctx.tenant_name)
        .ok_or_else(|| not_found(&format!("space '{name}' doesn't exist")))?;
    if !ctx.state.is_ancestor_or_self(home, s.id) {
        return Err(denied(&format!(
            "you may act only in your home space '{}' and below it, not in '{name}'",
            ctx.space_name(home)
        )));
    }
    Ok(s.id)
}

/// `CREATE SPACE s [UNDER p]`.
///
/// # Errors
/// 1227; 39880; 39881; 39883; 39884.
pub async fn create_space(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    name: &str,
    parent: Option<&str>,
) -> Result<Done> {
    need_space_admin(ctx)?;
    let parent = match parent {
        Some(p) => ctx
            .state
            .space_named(p, ctx.tenant_name)
            .ok_or_else(|| not_found(&format!("space '{p}' doesn't exist")))?,
        None => ctx
            .state
            .space(ctx.space, ctx.tenant_name)
            .ok_or_else(|| not_found("the session's space doesn't exist"))?,
    };
    if !ctx.state.is_ancestor_or_self(ctx.space, parent.id) {
        return Err(denied(&format!(
            "a space can be created only at or below the session's space '{}'",
            ctx.space_name(ctx.space)
        )));
    }
    if ctx.state.space_named(name, ctx.tenant_name).is_some()
        || name.eq_ignore_ascii_case(ctx.tenant_name)
    {
        return Err(SprigError::new(
            SprigCode::SpaceExists,
            format!("space '{name}' exists"),
        ));
    }
    let depth = ctx.cat.ancestry.len() + usize::from(parent.depth) + 1;
    if depth > ctx.limits.tenant_max_depth {
        return Err(SprigError::new(
            SprigCode::SpaceDepthExceeded,
            format!(
                "space '{name}' would be {depth} levels deep (tenant.max_depth is {})",
                ctx.limits.tenant_max_depth
            ),
        ));
    }
    let mut c = root_change(ctx);
    c.add_spaces.push(NewSpace {
        name: name.to_owned(),
        parent: parent.id,
        created_by: ctx.user.unwrap_or(0),
    });
    r.commit(&c).await?;
    Ok(Done::default())
}

/// `DROP SPACE [IF EXISTS] s [CASCADE]`.
///
/// # Errors
/// 1227; 39880; 39882 for the root, children or layer rows without
/// `CASCADE`; 39884.
pub async fn drop_space(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    name: &str,
    if_exists: bool,
    cascade: bool,
) -> Result<Done> {
    need_space_admin(ctx)?;
    let Some(sp) = ctx.state.space_named(name, ctx.tenant_name) else {
        if if_exists {
            return Ok(Done::default());
        }
        return Err(not_found(&format!("space '{name}' doesn't exist")));
    };
    if sp.id == ROOT_SPACE {
        return Err(SprigError::new(
            SprigCode::SpaceHasChildren,
            "the root space can't be dropped",
        ));
    }
    if sp.id == ctx.space || !ctx.state.is_ancestor_or_self(ctx.space, sp.id) {
        return Err(denied(&format!(
            "space '{name}' is not below the session's space '{}'",
            ctx.space_name(ctx.space)
        )));
    }
    let mut all = vec![sp.id];
    all.extend(ctx.state.descendants(sp.id));
    if all.len() > 1 && !cascade {
        return Err(SprigError::new(
            SprigCode::SpaceHasChildren,
            format!("space '{name}' has child spaces: use CASCADE"),
        ));
    }
    let list = all
        .iter()
        .map(|s| s.0.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    for o in ctx.state.objects() {
        let Some(db) = ctx.db_name(o.db) else {
            continue;
        };
        let layer = format!("{}.{}", q(&db), q(&o.table));
        if cascade {
            r.exec(&format!(
                "DELETE FROM {layer} WHERE `_sprig_space` IN ({list})"
            ))
            .await?;
        } else {
            let n = r
                .query(&format!(
                    "SELECT 1 FROM {layer} WHERE `_sprig_space` IN ({list}) LIMIT 1"
                ))
                .await?;
            if !n.rows.is_empty() {
                return Err(SprigError::new(
                    SprigCode::SpaceHasChildren,
                    format!("space '{name}' has rows in {db}.{}: use CASCADE", o.table),
                ));
            }
        }
    }
    let mut c = root_change(ctx);
    c.drop_spaces = all;
    r.commit(&c).await?;
    Ok(Done::default())
}

/// `ALTER SPACE s SET HOME FOR users`.
///
/// # Errors
/// 1227; 39880; 39884; 1396 for an unknown user.
pub async fn space_home(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    space: &str,
    users: &[String],
) -> Result<Done> {
    need_space_admin(ctx)?;
    let sp = ctx
        .state
        .space_named(space, ctx.tenant_name)
        .ok_or_else(|| not_found(&format!("space '{space}' doesn't exist")))?;
    if !ctx.state.is_ancestor_or_self(ctx.space, sp.id) {
        return Err(denied(&format!(
            "space '{space}' is not at or below the session's space"
        )));
    }
    let mut c = root_change(ctx);
    for u in users {
        let id = r.user_id(u).ok_or_else(|| {
            SprigError::standard(
                1396,
                *b"HY000",
                format!("Operation ALTER SPACE failed for '{u}'"),
            )
        })?;
        c.homes.push((id, (sp.id != ROOT_SPACE).then_some(sp.id)));
    }
    r.commit(&c).await?;
    Ok(Done::default())
}

/// `ALTER TABLE t SPRIG INHERITABLE KEY (k) [TAG FIELD f]`.
///
/// The table's MariaDB foreign keys, to and from it, become foreign keys
/// the gateway keeps, and the tables at their other ends become inheritable
/// too (their primary key is their key): a table and the tables linked to
/// it by foreign keys inherit together (Kiran's decision 1, [`crate::links`]).
///
/// # Errors
/// 1146; 1142; 39891 when the key isn't the primary key, or for a typed
/// table.
pub async fn inheritable(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    table: &Name,
    key: &[String],
    tag: Option<&str>,
) -> Result<Done> {
    let Some((db, db_name, object_id)) = ctx.object_id(table) else {
        return Err(no_table(table, ctx));
    };
    if let Some(o) = ctx.state.object(object_id) {
        let same = o.key.len() == key.len()
            && o.key
                .iter()
                .zip(key)
                .all(|(a, b)| a.eq_ignore_ascii_case(b));
        if same {
            // Again on a table of a type tree: the type layer changed its
            // columns or added a table to the tree (typed tables in
            // groups), which are followed here.
            if o.type_root.is_some() || ctx.type_tree(db, &table.name).is_some() {
                return follow_tree(ctx, r, db, &db_name, table, key).await;
            }
            return Ok(Done::default());
        }
        return Err(unsupported(format!(
            "{} is already inheritable with KEY ({})",
            table.name,
            o.key.join(", ")
        )));
    }
    if !ctx.privileges.alter(&db_name, db, object_id) {
        return Err(alter_denied(&db_name, &table.name));
    }
    let objects = crate::links::convert_native(ctx, r, db, &db_name, table, key, tag).await?;
    let mut c = root_change(ctx);
    c.upsert_objects = objects;
    r.commit(&c).await?;
    Ok(Done::default())
}

/// `SPRIG INHERITABLE` again on a table of an inheritable type tree (the
/// type layer's call after its DDL): new tables of the tree become
/// inheritable ([`crate::links::convert_native`]), and new columns of the
/// tree's tables get field slots.
async fn follow_tree(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db: sprig_common::ids::DbId,
    db_name: &str,
    table: &Name,
    key: &[String],
) -> Result<Done> {
    let mut objects = crate::links::convert_native(ctx, r, db, db_name, table, key, None).await?;
    for o in &mut objects {
        let layer = format!("{}.{}", q(db_name), q(&o.table));
        let cols: Vec<String> = crate::guard::show_columns(r, &layer)
            .await?
            .into_iter()
            .filter(|c| !c.generated() && !c.name.to_ascii_lowercase().starts_with("_sprig_"))
            .map(|c| c.name)
            .collect();
        // Type columns a discriminator made generated (decision T19)
        // aren't fields any more: merged views compute them.
        let gone = |c: &String| {
            (c.eq_ignore_ascii_case("_type") || c.eq_ignore_ascii_case("_type_path"))
                && !cols.iter().any(|x| x.eq_ignore_ascii_case(c))
        };
        if cols.iter().any(|c| o.column(c).is_none()) || o.columns.iter().any(gone) {
            let mut all: Vec<String> = o.columns.iter().filter(|c| !gone(c)).cloned().collect();
            all.extend(cols.into_iter().filter(|c| o.column(c).is_none()));
            if let Some(n) = o.with_slots_for(&all) {
                *o = n;
                o.version += 1;
            }
        }
    }
    let changed: Vec<InheritableObject> = objects
        .into_iter()
        .filter(|o| ctx.state.object(o.object_id).is_none_or(|x| **x != *o))
        .collect();
    if changed.is_empty() {
        return Ok(Done::default());
    }
    let mut c = root_change(ctx);
    c.upsert_objects = changed;
    r.commit(&c).await?;
    Ok(Done::default())
}

/// Makes table `table` (database `db`) inheritable in MariaDB: the layer
/// columns, the widened keys and `_sprig_layer`, in one `ALTER TABLE`.
/// `key` `None`: the primary key. Returns the object, not yet committed.
/// The object id comes from the runner, so a table created by the same
/// statement is found.
///
/// # Errors
/// 1146; 1072; 39891 when the key isn't the primary key, or for a typed
/// table.
pub(crate) async fn enable(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db: sprig_common::ids::DbId,
    table: &Name,
    key: Option<&[String]>,
    tag: Option<&str>,
) -> Result<InheritableObject> {
    let Some((_, object_id)) = r.object_id(table.db.as_deref(), &table.name).await? else {
        return Err(no_table(table, ctx));
    };
    if ctx
        .types()
        .is_some_and(|t| sprig_types::has_hierarchy(t, db, &table.name, ctx.lctn))
    {
        return Err(unsupported(
            "a table with a record hierarchy can't be inheritable yet",
        ));
    }
    let type_root = ctx.type_tree(db, &table.name).map(|(root, _)| root);
    let layer = match &table.db {
        Some(d) => format!("{}.{}", q(d), q(&table.name)),
        None => q(&table.name),
    };
    let old = crate::guard::show_columns(r, &layer).await?;
    // Generated columns (a discriminated root's type columns) aren't
    // fields: merged views compute them ([`Ctx::derived`]).
    let cols: Vec<String> = old
        .iter()
        .filter(|c| !c.generated())
        .map(|c| c.name.clone())
        .collect();
    let (pk, uniques) = crate::guard::show_uniques(r, &layer).await?;
    let key: Vec<String> = match key {
        Some(k) => k.to_vec(),
        None if !pk.is_empty() => pk.clone(),
        None => {
            return Err(unsupported(format!(
                "{} is linked to an inheritable table by a foreign key, so it inherits with it, \
                 and needs a primary key",
                table.name
            )));
        }
    };
    let mut key_cols = Vec::with_capacity(key.len());
    for k in &key {
        let c = cols
            .iter()
            .find(|c| c.eq_ignore_ascii_case(k))
            .ok_or_else(|| {
                SprigError::standard(
                    1072,
                    *b"42000",
                    format!("Key column '{k}' doesn't exist in table"),
                )
            })?;
        key_cols.push(c.clone());
    }
    let pk_matches = pk.len() == key_cols.len()
        && pk
            .iter()
            .zip(&key_cols)
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
    if !pk_matches {
        return Err(unsupported(format!(
            "SPRIG INHERITABLE KEY must be the primary key of {} ({})",
            table.name,
            if pk.is_empty() {
                "it has none".to_owned()
            } else {
                pk.join(", ")
            }
        )));
    }
    if let Some(t) = tag
        && !cols.iter().any(|c| c.eq_ignore_ascii_case(t))
    {
        return Err(SprigError::standard(
            1054,
            *b"42S22",
            format!("Unknown column '{t}' in 'TAG FIELD'"),
        ));
    }
    let klist = key_cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    let mut ops = vec![
        "ADD COLUMN `_sprig_space` BIGINT UNSIGNED NOT NULL DEFAULT 1 INVISIBLE".to_owned(),
        "ADD COLUMN `_sprig_state` ENUM('own','ovr','hide','orphan') NOT NULL DEFAULT 'own' \
         INVISIBLE"
            .to_owned(),
        format!("ADD COLUMN `_sprig_ovr` BIGINT UNSIGNED NOT NULL DEFAULT {ALL_BITS} INVISIBLE"),
        "DROP PRIMARY KEY".to_owned(),
        format!("ADD PRIMARY KEY ({klist}, `_sprig_space`)"),
    ];
    for u in &uniques {
        ops.push(format!("DROP INDEX {}", q(&u.name)));
        ops.push(format!(
            "ADD UNIQUE KEY {} ({}, `_sprig_space`)",
            q(&u.name),
            u.columns
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    ops.push(format!(
        "ADD KEY `_sprig_layer` (`_sprig_space`, `_sprig_state`, {klist})"
    ));
    r.exec(&format!("ALTER TABLE {layer} {}", ops.join(", ")))
        .await?;
    let empty = InheritableObject {
        object_id,
        db,
        table: table.name.clone(),
        key: key_cols,
        columns: Vec::new(),
        slots: std::collections::BTreeMap::new(),
        uniques,
        tag_field: tag.map(str::to_owned),
        version: 1,
        auto: crate::guard::auto_column(&old),
        links: Vec::new(),
        adopted: false,
        type_root,
    };
    Ok(empty.with_slots_for(&cols).unwrap_or(empty))
}

/// `ALTER TABLE t DROP SPRIG INHERITABLE`: back to a plain table, when
/// unshared and only the root's own rows are left.
///
/// # Errors
/// 39885; 39895; 39891 while other spaces have rows.
pub async fn drop_inheritable(ctx: &Ctx<'_>, r: &mut dyn Runner, table: &Name) -> Result<Done> {
    let Some(res) = ctx.resolve(table)? else {
        return Err(SprigError::new(
            SprigCode::NotInheritable,
            format!("{} is not inheritable", table.name),
        ));
    };
    if !ctx
        .privileges
        .alter(&res.db_name, res.db, res.obj.object_id)
    {
        return Err(alter_denied(&res.db_name, &table.name));
    }
    crate::guard::refuse_if_shared(ctx, &res.obj, "DROP SPRIG INHERITABLE")?;
    let left = r
        .query(&format!(
            "SELECT 1 FROM {} WHERE `_sprig_space` <> 1 OR `_sprig_state` NOT IN ('own', \
             'orphan') LIMIT 1",
            res.layer
        ))
        .await?;
    if !left.rows.is_empty() {
        return Err(unsupported(
            "other spaces have rows in this object: SPRIG RESET them or DROP SPACE ... CASCADE \
             first",
        ));
    }
    let o = &res.obj;
    let klist = o.key.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    let mut ops = vec![
        "DROP INDEX `_sprig_layer`".to_owned(),
        "DROP PRIMARY KEY".to_owned(),
        format!("ADD PRIMARY KEY ({klist})"),
    ];
    for u in &o.uniques {
        ops.push(format!("DROP INDEX {}", q(&u.name)));
        ops.push(format!(
            "ADD UNIQUE KEY {} ({})",
            q(&u.name),
            u.columns
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    ops.extend(
        ["`_sprig_space`", "`_sprig_state`", "`_sprig_ovr`"].map(|c| format!("DROP COLUMN {c}")),
    );
    // Refused again under the catalog lock if a share appeared meanwhile.
    let alter = format!("ALTER TABLE {} {}", res.layer, ops.join(", "));
    crate::guard::DESTRUCTIVE
        .scope("DROP SPRIG INHERITABLE".to_owned(), r.exec(&alter))
        .await?;
    let mut c = root_change(ctx);
    c.delete_objects.push(o.object_id);
    r.commit(&c).await?;
    Ok(Done::default())
}

pub(crate) fn alter_denied(db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("ALTER command denied for table `{db}`.`{table}`"),
    )
}

const NONDETERMINISTIC: &[&str] = &[
    "RAND",
    "UUID",
    "UUID_SHORT",
    "SYS_GUID",
    "NOW",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURDATE",
    "CURTIME",
    "SYSDATE",
    "UNIX_TIMESTAMP",
    "UTC_TIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "USER",
    "CURRENT_USER",
    "CURRENT_ROLE",
    "SESSION_USER",
    "SYSTEM_USER",
    "CONNECTION_ID",
    "LAST_INSERT_ID",
    "FOUND_ROWS",
    "ROW_COUNT",
    "DATABASE",
    "SCHEMA",
    "NEXTVAL",
    "LASTVAL",
    "SETVAL",
    "SLEEP",
    "BENCHMARK",
    "GET_LOCK",
    "RELEASE_LOCK",
    "LOAD_FILE",
    "VERSION",
    "SPRIG_ORIGIN",
];

fn filter_invalid(why: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ShareFilterInvalid, why.into())
}

/// Finds what a filter may not use.
struct FilterCheck<'a> {
    o: &'a InheritableObject,
    shared: Option<&'a [String]>,
    err: Option<String>,
}
impl Visitor for FilterCheck<'_> {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &Expr) -> std::ops::ControlFlow<()> {
        let col = match e {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                self.err = Some("a share filter can't have a subquery".into());
                return std::ops::ControlFlow::Break(());
            }
            Expr::Function(f) => {
                let n = f.name.to_string().to_ascii_uppercase();
                if NONDETERMINISTIC.contains(&n.as_str()) {
                    self.err = Some(format!("{n}() is not deterministic"));
                    return std::ops::ControlFlow::Break(());
                }
                return std::ops::ControlFlow::Continue(());
            }
            Expr::Identifier(i) => i.value.clone(),
            Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.clone()).unwrap_or_default(),
            _ => return std::ops::ControlFlow::Continue(()),
        };
        let Some(c) = self.o.column(&col) else {
            self.err = Some(format!("'{col}' is not a column of {}", self.o.table));
            return std::ops::ControlFlow::Break(());
        };
        let ok = self.o.is_key(c)
            || self
                .shared
                .is_none_or(|s| s.iter().any(|x| x.eq_ignore_ascii_case(c)));
        if !ok {
            self.err = Some(format!("'{c}' is not a shared column"));
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Checks a share or take filter (39888): deterministic, no subquery, no
/// variables, and only (shared) columns of the object.
///
/// # Errors
/// 39888.
pub fn validate_filter(
    o: &InheritableObject,
    filter: &str,
    shared: Option<&[String]>,
    lex: &sprig_sql::lex::LexOptions,
) -> Result<()> {
    use sprig_sql::lex::TokenKind;
    let toks =
        sprig_sql::lex::lex(filter.as_bytes(), lex).map_err(|_| filter_invalid("too long"))?;
    if toks.iter().any(|t| {
        matches!(
            t.kind,
            TokenKind::UserVar | TokenKind::SysVar | TokenKind::Param
        )
    }) {
        return Err(filter_invalid(
            "a share filter can't read variables or parameters",
        ));
    }
    let e = parse_expr(filter).map_err(|e| filter_invalid(e.message))?;
    let mut v = FilterCheck {
        o,
        shared,
        err: None,
    };
    let _ = e.visit(&mut v);
    match v.err {
        Some(why) => Err(filter_invalid(why)),
        None => Ok(()),
    }
}

/// `CREATE SHARE`'s parts.
#[derive(Debug)]
pub struct NewShare<'a> {
    /// The name.
    pub name: &'a str,
    /// The object.
    pub table: &'a Name,
    /// Where it goes.
    pub to: &'a To,
    /// The row filter.
    pub filter: Option<&'a str>,
    /// The shared columns.
    pub columns: Option<&'a [String]>,
    /// `WITH RESHARE`.
    pub reshare: bool,
}

/// `CREATE SHARE` from the session's space.
///
/// # Errors
/// 1142; 39880; 39881; 39883; 39885; 39888; 39889; 39890.
pub async fn create_share(ctx: &Ctx<'_>, r: &mut dyn Runner, s: &NewShare<'_>) -> Result<Done> {
    let Some(res) = ctx.resolve(s.table)? else {
        if ctx.object_id(s.table).is_some() {
            return Err(SprigError::new(
                SprigCode::NotInheritable,
                format!(
                    "{} is not inheritable: ALTER TABLE ... SPRIG INHERITABLE KEY (...) first",
                    s.table.name
                ),
            ));
        }
        return Err(no_table(s.table, ctx));
    };
    let o = &res.obj;
    if !ctx.privileges.alter(&res.db_name, res.db, o.object_id) {
        return Err(alter_denied(&res.db_name, &o.table));
    }
    if ctx.limits.merge_unbuilt {
        return Err(SprigError::new(
            SprigCode::ShareResidencyConflict,
            "inherit.merge asks for a federated edge, which this release doesn't build: set it \
             to auto, read_through or materialized",
        ));
    }
    if ctx.state.share_named(s.name).is_some() {
        return Err(SprigError::new(
            SprigCode::SpaceExists,
            format!("a share named '{}' exists", s.name),
        ));
    }
    let from = ctx.space;
    let to = match s.to {
        To::AllChildren => ShareTarget::AllChildren,
        To::Space(n) => match ctx.state.space_named(n, ctx.tenant_name) {
            Some(sp) if sp.parent == Some(from) => ShareTarget::Space(sp.id),
            Some(_) => {
                return Err(not_found(&format!(
                    "space '{n}' is not a child of the session's space '{}'",
                    ctx.space_name(from)
                )));
            }
            None if from == ROOT_SPACE => match r.child_tenant(n).await? {
                Some(t) => ShareTarget::Tenant(t),
                None => {
                    return Err(not_found(&format!(
                        "'{n}' is neither a child space nor a child tenant"
                    )));
                }
            },
            None => return Err(not_found(&format!("space '{n}' doesn't exist"))),
        },
    };
    // Re-sharing what this space inherits needs WITH RESHARE upstream.
    let chain = ctx.chain(o, from, &res.prefix)?;
    let upstream_reshare = if let Some(e) = ctx.state.edge(o.object_id, from) {
        Some(e.reshare)
    } else if from == ROOT_SPACE && matches!(chain.top, Top::Upstream { .. }) {
        ctx.up_edge(o)?.map(|u| u.share.reshare)
    } else {
        None
    };
    if upstream_reshare == Some(false) {
        return Err(SprigError::new(
            SprigCode::ShareNotResharable,
            format!(
                "space '{}' receives {} without WITH RESHARE",
                ctx.space_name(from),
                o.table
            ),
        ));
    }
    let mut columns = None;
    if let Some(cs) = s.columns {
        let mut out = Vec::new();
        for c in cs {
            let col = o
                .column(c)
                .ok_or_else(|| filter_invalid(format!("'{c}' is not a column of {}", o.table)))?;
            if !out.iter().any(|x: &String| x.eq_ignore_ascii_case(col)) {
                out.push(col.to_owned());
            }
        }
        for k in &o.key {
            if !out.iter().any(|x| x.eq_ignore_ascii_case(k)) {
                out.insert(0, k.clone());
            }
        }
        columns = Some(out);
    }
    if let Some(f) = s.filter {
        validate_filter(o, f, columns.as_deref(), &ctx.lex)?;
    }
    let edges = ctx.chain_edges(o, from)? + 1;
    if edges > ctx.limits.max_depth {
        return Err(SprigError::new(
            SprigCode::SpaceDepthExceeded,
            format!(
                "the share would make a chain of {edges} edges; inherit.max_depth is {}",
                ctx.limits.max_depth
            ),
        ));
    }
    // Read-through levels since the nearest materialized edge (chapter 42,
    // "Choosing the merge mode").
    let levels = ctx
        .chain_with(o, from, &res.prefix, crate::plan::Cut::Declared)?
        .levels
        .len()
        + 1;
    let deep = levels > ctx.limits.read_through_max_depth + 1;
    let mode = match ctx.limits.merge {
        Some(MergeMode::Materialized) => MergeMode::Materialized,
        Some(_) if deep => {
            return Err(SprigError::new(
                SprigCode::SpaceDepthExceeded,
                format!(
                    "the share would make {levels} read-through levels in this tenant; \
                     inherit.read_through_max_depth is {} (inherit.merge = auto or \
                     materialized makes this edge materialized)",
                    ctx.limits.read_through_max_depth
                ),
            ));
        }
        None if deep => MergeMode::Materialized,
        Some(_) | None => MergeMode::ReadThrough,
    };
    let mut c = root_change(ctx);
    c.upsert_shares.push(Share {
        id: 0,
        name: s.name.to_owned(),
        object_id: o.object_id,
        from,
        to,
        filter: s.filter.map(str::to_owned),
        columns,
        take: None,
        reshare: s.reshare,
        version: 1,
        created_by: ctx.user.unwrap_or(0),
        mode,
        via: None,
    });
    r.commit(&c).await?;
    // The copy worker keeps copies and orphans across tenants.
    let crosses = matches!(to, ShareTarget::Tenant(_))
        || (to == ShareTarget::AllChildren && from == ROOT_SPACE);
    if (mode == MergeMode::Materialized || crosses)
        && let Err(e) = r.subscribe().await
    {
        tracing::warn!(error = %e, share = s.name, "the copy worker's subscription wasn't registered");
    }
    Ok(Done::default())
}

/// `ALTER SHARE n WHERE e` (the parent) or `TAKE WHERE e | TAKE ALL` (the
/// child).
///
/// # Errors
/// 1142; 39884; 39885; 39887; 39888; 39891.
pub async fn alter_share(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    name: &str,
    change: &ShareChange,
) -> Result<Done> {
    let own = ctx.state.share_named(name).cloned();
    match change {
        ShareChange::Filter(f) => {
            let Some(sh) = own else {
                return Err(SprigError::new(
                    SprigCode::ShareNotFound,
                    format!("share '{name}' doesn't exist here (a received share takes TAKE)"),
                ));
            };
            if !ctx.state.is_ancestor_or_self(ctx.space, sh.from) {
                return Err(denied(&format!(
                    "share '{name}' belongs to space '{}'",
                    ctx.space_name(sh.from)
                )));
            }
            let o = ctx
                .state
                .object(sh.object_id)
                .ok_or_else(|| SprigError::internal("a share of a missing object"))?;
            let db = ctx.db_name(o.db).unwrap_or_default();
            if !ctx.privileges.alter(&db, o.db, o.object_id) {
                return Err(alter_denied(&db, &o.table));
            }
            validate_filter(o, f, sh.columns.as_deref(), &ctx.lex)?;
            let mut n = (*sh).clone();
            n.filter = Some(f.clone());
            n.version += 1;
            let mut c = root_change(ctx);
            c.upsert_shares.push(n);
            r.commit(&c).await?;
            Ok(Done::default())
        }
        ShareChange::Take(t) => {
            // A share made to this space by name, in this tenant.
            if let Some(sh) = own {
                if sh.to != ShareTarget::Space(ctx.space) {
                    return Err(unsupported(
                        "TAKE narrows a share made to the session's space by name (TO SPACE)",
                    ));
                }
                let o = ctx
                    .state
                    .object(sh.object_id)
                    .ok_or_else(|| SprigError::internal("a share of a missing object"))?;
                let db = ctx.db_name(o.db).unwrap_or_default();
                if !ctx.privileges.alter(&db, o.db, o.object_id) {
                    return Err(alter_denied(&db, &o.table));
                }
                if let Some(f) = t {
                    validate_filter(o, f, sh.columns.as_deref(), &ctx.lex)?;
                }
                let mut n = (*sh).clone();
                n.take.clone_from(t);
                n.version += 1;
                let mut c = root_change(ctx);
                c.upsert_shares.push(n);
                r.commit(&c).await?;
                return Ok(Done::default());
            }
            // A share from the parent tenant.
            let Some(parent) = ctx.ancestors.last() else {
                return Err(SprigError::new(
                    SprigCode::ShareNotFound,
                    format!("share '{name}' doesn't exist"),
                ));
            };
            let ps = state_of(parent).and_then(|s| s.share_named(name).cloned());
            let Some(sh) = ps else {
                return Err(SprigError::new(
                    SprigCode::ShareNotFound,
                    format!("share '{name}' doesn't exist"),
                ));
            };
            if ctx.space != ROOT_SPACE {
                return Err(denied(
                    "a share from the parent tenant is narrowed in the root space",
                ));
            }
            let mine = ctx.state.objects().find(|o| {
                ctx.up_edge(o)
                    .ok()
                    .flatten()
                    .is_some_and(|u| u.share.id == sh.id)
            });
            let Some(o) = mine else {
                return Err(SprigError::new(
                    SprigCode::NotInheritable,
                    format!(
                        "share '{name}' doesn't reach an inheritable object of this tenant: \
                         declare the object SPRIG INHERITABLE first"
                    ),
                ));
            };
            let db = ctx.db_name(o.db).unwrap_or_default();
            if !ctx.privileges.alter(&db, o.db, o.object_id) {
                return Err(alter_denied(&db, &o.table));
            }
            if let Some(f) = t {
                validate_filter(o, f, sh.columns.as_deref(), &ctx.lex)?;
            }
            let mut c = root_change(ctx);
            c.upsert_takes.push(Take {
                share_id: sh.id,
                peer_tenant: parent.tenant,
                object_id: o.object_id,
                take: t.clone(),
            });
            r.commit(&c).await?;
            Ok(Done::default())
        }
    }
}

/// `DROP SHARE [IF EXISTS] n`.
///
/// # Errors
/// 1142; 39884; 39887.
pub async fn drop_share(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    name: &str,
    if_exists: bool,
) -> Result<Done> {
    let Some(sh) = ctx.state.share_named(name).cloned() else {
        if if_exists {
            return Ok(Done::default());
        }
        return Err(SprigError::new(
            SprigCode::ShareNotFound,
            format!("share '{name}' doesn't exist"),
        ));
    };
    if !ctx.state.is_ancestor_or_self(ctx.space, sh.from) {
        return Err(denied(&format!(
            "share '{name}' belongs to space '{}'",
            ctx.space_name(sh.from)
        )));
    }
    if let Some(o) = ctx.state.object(sh.object_id) {
        let db = ctx.db_name(o.db).unwrap_or_default();
        if !ctx.privileges.alter(&db, o.db, o.object_id) {
            return Err(alter_denied(&db, &o.table));
        }
    }
    // The children it served, read before the drop (bugfix-44).
    let carried = crate::unshare::carried(ctx, &sh);
    let mut c = root_change(ctx);
    c.drop_shares.push(sh.id);
    r.commit(&c).await?;
    // The share is gone: its children's overrides become their own rows
    // and its views go. A failure leaves them as before (hidden, the view
    // unused), so the statement still succeeds.
    match crate::unshare::after_drop(ctx, r, &carried).await {
        Ok(u) if u.views + u.orphaned > 0 => tracing::info!(
            share = %name,
            views = u.views,
            orphaned = u.orphaned,
            "share dropped: the children's overrides are their own rows"
        ),
        Ok(_) => {}
        Err(e) => {
            metrics::counter!("sprig_inherit_unshare_failures_total").increment(1);
            tracing::warn!(share = %name, error = %e, "share dropped, but its children were not cleaned up");
        }
    }
    Ok(Done::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fixtures;

    #[test]
    fn filters() {
        let o = fixtures::products();
        let lex = sprig_sql::lex::LexOptions::mariadb();
        validate_filter(&o, "region = 'IN' AND price < 100", None, &lex).unwrap();
        validate_filter(&o, "UPPER(region) IN ('IN', 'LK')", None, &lex).unwrap();
        let bad = |f: &str, shared: Option<&[String]>| {
            validate_filter(&o, f, shared, &lex)
                .unwrap_err()
                .code()
                .unwrap()
        };
        assert_eq!(bad("price < RAND()", None), SprigCode::ShareFilterInvalid);
        assert_eq!(bad("id IN (SELECT 1)", None), SprigCode::ShareFilterInvalid);
        assert_eq!(bad("nope = 1", None), SprigCode::ShareFilterInvalid);
        assert_eq!(bad("price < @x", None), SprigCode::ShareFilterInvalid);
        assert_eq!(
            bad("price < 10", Some(&["name".to_owned()])),
            SprigCode::ShareFilterInvalid
        );
        validate_filter(&o, "id > 3", Some(&["name".to_owned()]), &lex).unwrap();
    }
}
