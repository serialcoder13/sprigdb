//! The gateway's [`DdlExecutor`] (chapter 08, "DDL write path", phase 2):
//! physical DDL on the tenant's cluster.
//!
//! - **Object DDL** (tables, views, routines, triggers, events, indexes,
//!   renames, `TRUNCATE`) runs on the session's own backend session, as
//!   the session's backend account, in the session's state: its current
//!   database stays the one the client chose, so unqualified names inside
//!   a view or routine body mean what they mean on MySQL (resolved against
//!   the default database at creation), and a statement inside an open
//!   transaction commits it implicitly, as MySQL's DDL does. The rewrite
//!   has already made every qualified name physical.
//! - **Stored programs** (procedures, functions, triggers, events) run as
//!   `sprig_ctl` too: see `stored_program`.
//! - **Database DDL** (`CREATE`, `ALTER`, `DROP DATABASE`) runs as the
//!   cluster's provisioning account `sprig_ctl`: creating a `t<id>_d...`
//!   schema needs a server-level privilege no tenant's backend account
//!   should hold (R0; chapter 29 derives per-tenant grants in M0.5).
//! - **Reads** for phase 3 (`SHOW CREATE`, the schema's defaults) and the
//!   reconciler's questions go through `sprig_ctl` on the writer too, so
//!   they never disturb the session's state.
//!
//! Errors keep the DDL path's meaning: a server error is definite (MariaDB
//! DDL is atomic); a statement that never left the gateway (no connection,
//! pool timeout) is 39030, also definite; anything after sending leaves the
//! outcome unknown for the reconciler.

use sprig_catalog::ddl::{
    DdlExecutor, Effect, MariaExecutor, PhysicalDdl, PhysicalObject, SchemaInfo,
};
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::TenantId;
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_proto_mysql::client::PacketKind;
use tokio::sync::Mutex;

use crate::backend::{BackendSession, StmtInfo};

/// Physical DDL for one statement of one session (see the module docs).
pub struct GatewayDdl<'a> {
    bs: Mutex<&'a mut BackendSession>,
    ctl: MariaExecutor,
}

impl std::fmt::Debug for GatewayDdl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayDdl").finish_non_exhaustive()
    }
}

impl<'a> GatewayDdl<'a> {
    /// Object DDL on `bs`, database DDL and reads on `ctl`.
    pub fn new(bs: &'a mut BackendSession, ctl: MariaExecutor) -> Self {
        Self {
            bs: Mutex::new(bs),
            ctl,
        }
    }
}

impl GatewayDdl<'_> {
    async fn session_show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        use sprig_common::names::quote;
        let sql = format!(
            "SHOW CREATE {} {}.{}",
            sprig_catalog::ddl::kind_keyword(kind),
            quote(&schema.to_string()),
            quote(name)
        );
        let mut bs = self.bs.lock().await;
        bs.query(&StmtInfo::read(), sql.as_bytes()).await?;
        let mut text = None;
        let mut idx = None;
        let mut seen = 0usize;
        let mut cols = 0usize;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            match p.kind {
                PacketKind::ColumnCount(n) => cols = usize::try_from(n).unwrap_or(0),
                PacketKind::ColumnDef => {
                    let c = sprig_proto_mysql::ColumnDef::decode(
                        &p.payload,
                        bs.backend_caps()
                            .unwrap_or(sprig_proto_mysql::Caps::empty()),
                    )?;
                    if idx.is_none()
                        && (c.name.starts_with(b"Create ")
                            || &c.name[..] == b"SQL Original Statement")
                    {
                        idx = Some(seen);
                    }
                    seen += 1;
                }
                PacketKind::Row => {
                    let vals = sprig_proto_mysql::resultset::decode_text_row(&p.payload, cols)?;
                    if let Some(Some(v)) = idx.and_then(|i| vals.get(i)) {
                        text = Some(String::from_utf8_lossy(v).into_owned());
                    }
                }
                PacketKind::Err(e) => err = Some(e.code),
                _ => {}
            }
        }
        let _ = bs.finish().await;
        match err {
            // Missing table, view, routine, trigger, event, schema.
            Some(1146 | 1049 | 1305 | 1360 | 1539 | 4091) => Ok(None),
            Some(code) => Err(SprigError::internal(format!(
                "SHOW CREATE failed with {code}"
            ))),
            None => Ok(text),
        }
    }
}

/// Routines, triggers and events: with binary logging on, MariaDB refuses
/// to create them (1419) for an account without `SUPER`, unless
/// `log_bin_trust_function_creators` is on server-wide. R0 runs them as
/// `sprig_ctl` in the object's schema (the engine's `PhysicalDdl::schema`),
/// where unqualified names in the statement resolve as in the session (the
/// object's database is the session's current one unless qualified, and
/// qualified names are already physical). Their `DEFINER` is then the
/// provisioning account, which output mapping shows as the session's user.
fn stored_program(e: &Effect) -> bool {
    let k = match e {
        Effect::Create { kind, .. } | Effect::Alter { kind, .. } | Effect::Drop { kind, .. } => {
            *kind
        }
        _ => return false,
    };
    matches!(
        k,
        ObjectKind::Procedure | ObjectKind::Function | ObjectKind::Trigger | ObjectKind::Event
    )
}

/// An error before the statement reached a server: nothing ran.
fn not_sent(e: SprigError) -> SprigError {
    match e.code() {
        Some(
            SprigCode::BackendPoolTimeout
            | SprigCode::BackendUnavailable
            | SprigCode::SessionStateLost
            | SprigCode::ClusterUnavailable,
        ) => SprigError::new(SprigCode::BackendUnavailable, e.message.clone()).with_source(e),
        _ if matches!(
            e.kind,
            ErrorKind::Backend { .. } | ErrorKind::Standard { .. }
        ) =>
        {
            e
        }
        _ => SprigError::new(
            SprigCode::OutcomeUnknown,
            "the backend was lost during DDL; the outcome is unknown",
        )
        .with_source(e),
    }
}

#[async_trait::async_trait]
impl DdlExecutor for GatewayDdl<'_> {
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        if stmt.effect.on_schema() || stored_program(&stmt.effect) {
            return self.ctl.execute(stmt).await;
        }
        let mut bs = self.bs.lock().await;
        let info = StmtInfo::classify(stmt.sql.as_bytes());
        bs.query(&info, stmt.sql.as_bytes())
            .await
            .map_err(not_sent)?;
        let mut err = None;
        loop {
            match bs.next().await {
                Ok(Some(p)) => {
                    if let PacketKind::Err(e) = p.kind {
                        err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(SprigError::new(
                        SprigCode::OutcomeUnknown,
                        "the backend was lost during DDL; the outcome is unknown",
                    )
                    .with_source(e));
                }
            }
        }
        if let Err(e) = bs.finish().await
            && err.is_none()
        {
            tracing::debug!(error = %e, "finishing a DDL statement failed");
        }
        err.map_or(Ok(()), Err)
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        self.ctl.schema(schema).await
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        // On the session's own connection first: under `LOCK TABLES` (as a
        // dump's restore runs `ALTER TABLE t DISABLE KEYS`) the session holds
        // the metadata lock another connection's read would wait for.
        match self.session_show_create(schema, kind, name).await {
            Ok(r) => Ok(r),
            Err(e) => {
                tracing::debug!(error = %e, "SHOW CREATE on the session failed; reading as sprig_ctl");
                self.ctl.show_create(schema, kind, name).await
            }
        }
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        self.ctl.objects(schema).await
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        self.ctl.tenant_schemas(tenant).await
    }

    async fn busy(&self, schema: &SchemaName) -> Result<bool> {
        self.ctl.busy(schema).await
    }
}
