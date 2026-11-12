//! `sprig-inherit`: spaces, shares, layers and merged views.
//!
//! Layer: 2 data layer. Design: [chapter 42](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/42-data-inheritance.md) (Data inheritance) of the SprigDB low-level design.
//!
//! What exists (R6 order 2, SQL on single-shard tenants, read-through):
//!
//! - [`model`] and [`store`]: spaces, inheritable objects, shares and home
//!   spaces in the tenant catalog, loaded into every snapshot by
//!   [`InheritExtension`] (build step 1);
//! - [`grammar`]: the extension statements;
//! - [`ddl`]: spaces, shares, home spaces and `SPRIG INHERITABLE` (steps 1-3);
//! - [`view`] and [`rewrite`]: the merged view and statements reading it,
//!   with `SPRIG_ORIGIN()` (step 4);
//! - [`write`](mod@write): layer writes, resets, parent deletes and
//!   synchronous orphaning (steps 5 and 6);
//! - [`upstream`]: cross-tenant read-through edges, through share views
//!   in the child's schema defined by the parent's account (step 9, the
//!   read-through half);
//! - [`copy`]: materialized copies and asynchronous orphaning across
//!   tenants, kept by one worker per tenant tree from the change feed
//!   (step 8 and step 9's other half; the job is `sprig-worker::inherit`);
//! - [`links`]: foreign keys between inheritable tables, checked against
//!   merged views, and refused deletes of referenced rows (Kiran's
//!   decisions 1 and 4 of 11 November 2026);
//! - [`ids`]: one id sequence per table for the whole tree (decision 3);
//! - [`adopt`]: what a child tenant must create or change so it holds
//!   every table its parent shares to it (decisions 1 and 2);
//! - [`moves`]: the tenant-move check for shares (39361);
//! - [`guard`]: "Schema changes while an object is shared" (39895);
//! - [`show`]: what schema tools see;
//! - [`reference`]: the merged view as a Rust model, for tests;
//! - [`msg`]: inherited messaging objects (`FOR EACH TENANT`, R9): the
//!   definitions, overrides and instances every kind shares.
//!
//! [`handle`] is the entry point: the gateway calls it for each statement
//! of a tenant session before chapter 32's type layer and chapter 07's name
//! plan, with a [`Runner`] that sends the plan's statements back through
//! the session (logical names, chapter 29's checks, chapter 17's router).
//!
//! Not built: federated edges (Spider), collections and key-value
//! namespaces, `ViewMapper` for the feed.

// The plans read best as the chapter's steps in order, over SQL text built
// with format!; short names (o, s, r, k) follow the chapter's pseudocode.
#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::struct_excessive_bools
)]

pub mod adopt;
pub mod copy;
pub mod ddl;
pub mod grammar;
pub mod guard;
pub mod ids;
pub mod links;
pub mod model;
pub mod moves;
pub mod msg;
pub mod plan;
pub mod reference;
pub mod rewrite;
pub mod settings;
pub mod show;
pub mod store;
pub mod subs;
pub mod upstream;
pub mod view;
pub mod write;

use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_types::{Done, Rows};

pub use model::{InheritState, InheritableObject, LayerState, ROOT_SPACE, Share, Space, SpaceId};
pub use plan::{Ctx, Limits, Privileges};
pub use store::{INHERIT_MODULE, InheritChange, InheritExtension, InheritStore, MODULE};

use crate::grammar::Stmt;

/// What the inheritance layer needs from the executor, beyond chapter
/// 32's [`sprig_types::Runner`].
#[async_trait::async_trait]
pub trait Runner: sprig_types::Runner {
    /// Commits `change` to the tenant's `inherit` module in one versioned
    /// catalog change (migrating the module first if needed).
    ///
    /// # Errors
    /// The catalog's.
    async fn commit(&mut self, change: &InheritChange) -> Result<()>;

    /// Runs `sql` as the provisioning account on the session's cluster:
    /// the share views of cross-tenant edges.
    ///
    /// # Errors
    /// The backend's.
    async fn ctl(&mut self, sql: &str) -> Result<()>;

    /// The id of tenant `name` when it is a direct child of this one.
    ///
    /// # Errors
    /// The directory's.
    async fn child_tenant(&mut self, name: &str) -> Result<Option<TenantId>>;

    /// A user of this tenant by name.
    fn user_id(&self, name: &str) -> Option<u32>;

    /// Asks for the copy worker of this tenant's tree (chapter 42,
    /// "Materialized copies"): registers its feed subscription on this
    /// tenant and its ancestors, from their current positions, unless it
    /// exists. Called when a share needs the worker (a materialized share,
    /// or one that reaches child tenants, which are orphaned
    /// asynchronously).
    ///
    /// # Errors
    /// The directory's and the feed's.
    async fn subscribe(&mut self) -> Result<()> {
        Ok(())
    }

    /// Runs `sql` (physical names) as the provisioning account on tenant
    /// `t`'s cluster, outside the session's transaction; its rows as text.
    ///
    /// # Errors
    /// The backend's (a `NOWAIT` lock conflict is 1205 or 3572).
    async fn ctl_query(&mut self, t: TenantId, sql: &str) -> Result<RawRows> {
        let _ = (t, sql);
        Err(SprigError::internal(
            "no provisioning access in this runner",
        ))
    }

    /// Runs `sql` on `inherit_refs` in tenant `root`'s catalog (`{refs}`
    /// stands for the table, created when missing), as the provisioning
    /// account, autocommitted ([`links`]).
    ///
    /// # Errors
    /// The catalog's.
    async fn refs(&mut self, root: TenantId, sql: &str) -> Result<RawRows> {
        let _ = (root, sql);
        Err(SprigError::internal("no reference index in this runner"))
    }

    /// Tenant `t`'s snapshot.
    ///
    /// # Errors
    /// The cache's.
    async fn tenant_catalog(&mut self, t: TenantId) -> Result<Arc<TenantCatalog>> {
        let _ = t;
        Err(SprigError::internal("no catalog cache in this runner"))
    }

    /// `n` values of `seq` (the first and the step), after moving it past
    /// `explicit`, the largest explicit value of the statement ([`ids`]).
    /// `None`: the runner has no sequences (MariaDB's counter is used).
    ///
    /// # Errors
    /// 39460, 39463, 1467.
    async fn tree_ids(
        &mut self,
        seq: &ids::TreeSeq,
        n: u64,
        explicit: Option<i128>,
    ) -> Result<Option<(i128, i128)>> {
        let _ = (seq, n, explicit);
        Ok(None)
    }

    /// Every tenant in `root`'s subtree (`root` included) holding table
    /// `db.table`, with its physical schema.
    ///
    /// # Errors
    /// The directory's.
    async fn tree_tables(
        &mut self,
        root: TenantId,
        db: &str,
        table: &str,
    ) -> Result<Vec<ids::TreeTable>> {
        let _ = (root, db, table);
        Ok(Vec::new())
    }
}

/// Rows as text, as [`Runner::ctl_query`] returns them.
pub type RawRows = Vec<Vec<Option<Vec<u8>>>>;

/// What the gateway does with a statement.
#[derive(Debug)]
pub enum Handled {
    /// Not the inheritance layer's: the statement goes on unchanged.
    NotInherit,
    /// Send this statement (logical names, internal mode) instead; its
    /// result is the client's.
    Forward(String),
    /// The plan ran: answer OK.
    Ok(Done),
    /// The plan's answer: a result set.
    Rows(Rows),
    /// `USE SPACE`: the session's new space.
    Space(SpaceId),
    /// The statement names a typed table: chapter 32 plans it first, and
    /// its statements come back here ([`Ctx::from_types`]).
    Typed,
}

/// A quick test on the raw text: could the statement concern the
/// inheritance layer? False for every statement when the tenant has no
/// inheritable objects and the text has none of the extension keywords.
#[must_use]
pub fn maybe_inherit(sql: &[u8], state: Option<&InheritState>) -> bool {
    if state.is_some_and(|s| !s.is_empty()) {
        return true;
    }
    let upper = sql.to_ascii_uppercase();
    let has = |w: &[u8]| upper.windows(w.len()).any(|x| x == w);
    has(b"SPACE") || has(b"SHARE") || has(b"SPRIG") || has(b"INHERITABLE")
}

/// Handles one statement of a tenant session (see the crate docs).
///
/// # Errors
/// The chapter's errors (39880-39895), 1062, 1142 and the steps' errors
/// with logical names.
pub async fn handle(sql: &str, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    if !maybe_inherit(sql.as_bytes(), Some(ctx.state))
        || grammar::names_reserved(sql.as_bytes(), &ctx.lex)
    {
        return Ok(Handled::NotInherit);
    }
    let stmt = grammar::classify(sql.as_bytes(), &ctx.lex)?;
    // Extension statements first; then statements on objects.
    match stmt {
        Stmt::UseSpace(name) => return ddl::use_space(ctx, name.as_deref()).map(Handled::Space),
        Stmt::ShowSpaces => return Ok(Handled::Rows(show::spaces(ctx))),
        Stmt::ShowShares => return Ok(Handled::Rows(show::shares(ctx))),
        Stmt::ShowCreateShare(name) => return show::create_share(ctx, &name).map(Handled::Rows),
        Stmt::ShowInheritable => return show::inheritable(ctx).map(Handled::Rows),
        Stmt::CheckRefs { table, filter } => {
            return Box::pin(links::check_refs(ctx, r, &table, filter.as_deref()))
                .await
                .map(Handled::Rows);
        }
        Stmt::CreateSpace { name, parent } => {
            return ddl::create_space(ctx, r, &name, parent.as_deref())
                .await
                .map(Handled::Ok);
        }
        Stmt::DropSpace {
            name,
            if_exists,
            cascade,
        } => {
            return ddl::drop_space(ctx, r, &name, if_exists, cascade)
                .await
                .map(Handled::Ok);
        }
        Stmt::SpaceHome { space, users } => {
            return ddl::space_home(ctx, r, &space, &users)
                .await
                .map(Handled::Ok);
        }
        Stmt::Inheritable { table, key, tag } => {
            return ddl::inheritable(ctx, r, &table, &key, tag.as_deref())
                .await
                .map(Handled::Ok);
        }
        Stmt::DropInheritable(table) => {
            return ddl::drop_inheritable(ctx, r, &table).await.map(Handled::Ok);
        }
        Stmt::CreateShare {
            name,
            table,
            to,
            filter,
            columns,
            reshare,
        } => {
            return ddl::create_share(
                ctx,
                r,
                &ddl::NewShare {
                    name: &name,
                    table: &table,
                    to: &to,
                    filter: filter.as_deref(),
                    columns: columns.as_deref(),
                    reshare,
                },
            )
            .await
            .map(Handled::Ok);
        }
        Stmt::AlterShare { name, change } => {
            return ddl::alter_share(ctx, r, &name, &change)
                .await
                .map(Handled::Ok);
        }
        Stmt::DropShare { name, if_exists } => {
            return ddl::drop_share(ctx, r, &name, if_exists)
                .await
                .map(Handled::Ok);
        }
        Stmt::CheckIds(table) => return Box::pin(ids::check(ctx, r, &table)).await,
        Stmt::Reset {
            table,
            columns,
            filter,
        } => {
            return write::reset(ctx, r, &table, columns.as_deref(), filter.as_deref())
                .await
                .map(Handled::Ok);
        }
        // It names no object: checked before the reference test below.
        Stmt::DropDatabase(db) => {
            return guard::drop_database(&db, ctx).map(|()| Handled::NotInherit);
        }
        _ => {}
    }
    if ctx.state.objects().next().is_none() {
        return Ok(Handled::NotInherit);
    }
    let touches = plan::touches(sql, ctx);
    if !touches {
        return Ok(Handled::NotInherit);
    }
    if !ctx.from_types && plan::touches_typed(sql, ctx) {
        // A drop on an inheritable type tree meets the rules of shared and
        // received tables first (decision T18); chapter 32 then drops this
        // tenant's own tables, every space's rows included. A tree is one
        // group, so the named tables decide for a CASCADE's subtypes.
        if let Stmt::DropTable(names) = &stmt {
            guard::drop_tables(names, ctx)?;
        }
        // A tree's discriminator is its top tenant's: below, the tree
        // follows the parent's at the next statement (decision T19).
        if let Stmt::AlterTable(n) = &stmt
            && grammar::identifiers(sql.as_bytes(), &ctx.lex)
                .iter()
                .any(|i| i.eq_ignore_ascii_case("DISCRIMINATOR"))
            && let Some(res) = ctx.resolve(n)?
        {
            guard::refuse_if_received(ctx, &res.obj, "ADD | DROP DISCRIMINATOR")?;
        }
        return Ok(Handled::Typed);
    }
    // A type plan's DDL (type columns, extension tables) is the type
    // layer's; it makes new tables of an inheritable tree inheritable
    // itself (`SPRIG INHERITABLE`, above).
    if ctx.from_types {
        let type_columns = || {
            grammar::identifiers(sql.as_bytes(), &ctx.lex)
                .iter()
                .any(|i| i.eq_ignore_ascii_case("_type") || i.eq_ignore_ascii_case("_type_path"))
        };
        match &stmt {
            // A client's ALTER of a typed table, run by the type layer:
            // the schema-change rules of shared tables still apply.
            Stmt::AlterTable(_) if !type_columns() => {}
            Stmt::AlterTable(_)
            | Stmt::CreateTable(_)
            | Stmt::DropTable(_)
            | Stmt::Rename(_)
            | Stmt::Truncate(_)
            | Stmt::CreateOther => return Ok(Handled::NotInherit),
            _ => {}
        }
    }
    match stmt {
        Stmt::Query => Box::pin(rewrite::query(sql, ctx, r)).await,
        Stmt::Insert | Stmt::Replace => Box::pin(write::insert(sql, ctx, r)).await,
        Stmt::Update => Box::pin(write::update(sql, ctx, r)).await,
        Stmt::Delete => Box::pin(write::delete(sql, ctx, r)).await,
        Stmt::AlterTable(n) => {
            if let Some(h) = Box::pin(links::alter(sql, &n, ctx, r)).await? {
                return Ok(h);
            }
            Box::pin(guard::alter(sql, &n, ctx, r)).await
        }
        Stmt::CreateTable(n) => Box::pin(links::create_table(sql, &n, ctx, r)).await,
        Stmt::DropTable(names) => guard::drop_tables(&names, ctx).map(|()| Handled::NotInherit),
        Stmt::Rename(names) => guard::rename(&names, ctx).map(|()| Handled::NotInherit),
        Stmt::Truncate(n) => Box::pin(write::truncate(&n, ctx, r)).await,
        Stmt::ShowCreate(n) => Box::pin(show::create(&n, ctx, r)).await,
        Stmt::LoadData(n) => {
            if ctx.resolve(&n)?.is_some() {
                Err(grammar::unsupported(
                    "LOAD DATA into an inheritable object is not supported yet: use INSERT",
                ))
            } else {
                Ok(Handled::NotInherit)
            }
        }
        Stmt::CreateOther
            if !grammar::names(sql.as_bytes(), &ctx.lex)
                .iter()
                .any(|n| ctx.resolve(n).ok().flatten().is_some()) =>
        {
            Ok(Handled::NotInherit)
        }
        Stmt::CreateOther => Err(grammar::unsupported(
            "a view, routine, trigger or CREATE TABLE ... LIKE | SELECT naming an inheritable \
             object is not supported yet",
        )),
        _ => Ok(Handled::NotInherit),
    }
}
