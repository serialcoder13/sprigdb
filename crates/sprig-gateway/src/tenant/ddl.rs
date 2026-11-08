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
//!   `sprig_ctl` too, always with the session's own backend account as
//!   `DEFINER` (chapter 29, "Definers of stored code"): see
//!   `stored_program`.
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
use sprig_sql::lex::{LexOptions, TokenKind};
use tokio::sync::Mutex;

use crate::backend::{BackendSession, StmtInfo};

/// Physical DDL for one statement of one session (see the module docs).
pub struct GatewayDdl<'a> {
    bs: Mutex<&'a mut BackendSession>,
    ctl: MariaExecutor,
    /// `sprig_ctl`'s pool, to check stored programs' definers.
    ctl_pool: Option<mysql_async::Pool>,
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
            ctl_pool: None,
        }
    }

    /// Checks each stored program's recorded definer through `pool`
    /// (`sprig_ctl` on the same cluster) after creating it.
    #[must_use]
    pub fn checked_by(mut self, pool: mysql_async::Pool) -> Self {
        self.ctl_pool = Some(pool);
        self
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

impl GatewayDdl<'_> {
    /// The session's backend account as MariaDB authenticated it
    /// (`CURRENT_USER()`): user and host.
    async fn session_account(&self) -> Result<(String, String)> {
        let mut bs = self.bs.lock().await;
        bs.query(&StmtInfo::read(), b"SELECT CURRENT_USER()")
            .await
            .map_err(not_sent)?;
        let mut v = None;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            match p.kind {
                PacketKind::Row => {
                    let vals = sprig_proto_mysql::resultset::decode_text_row(&p.payload, 1)?;
                    if let Some(Some(x)) = vals.first() {
                        v = Some(String::from_utf8_lossy(x).into_owned());
                    }
                }
                PacketKind::Err(e) => {
                    err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                }
                _ => {}
            }
        }
        let _ = bs.finish().await;
        if let Some(e) = err {
            return Err(e);
        }
        v.as_deref()
            .and_then(|a| a.rsplit_once('@'))
            .map(|(u, h)| (u.to_owned(), h.to_owned()))
            .ok_or_else(|| SprigError::internal("CURRENT_USER() returned no account"))
    }

    /// A stored program: as `sprig_ctl`, with the session's account as
    /// `DEFINER`, then the definer MariaDB recorded is checked.
    async fn stored_program(&self, stmt: &PhysicalDdl) -> Result<()> {
        let (user, host) = self.session_account().await?;
        let definer = format!("DEFINER={}@{}", bq(&user), bq(&host));
        let sql = with_definer(&stmt.sql, &definer, &stmt.effect);
        let st = PhysicalDdl {
            sql,
            ..stmt.clone()
        };
        self.ctl.execute(&st).await?;
        self.verify_definer(stmt, &format!("{user}@{host}")).await
    }

    /// After `CREATE` (and `ALTER EVENT`): MariaDB must have recorded
    /// `want` as the definer; otherwise the object is dropped and the
    /// statement fails with 39367.
    async fn verify_definer(&self, stmt: &PhysicalDdl, want: &str) -> Result<()> {
        use mysql_async::prelude::Queryable;
        let (kind, name) = match &stmt.effect {
            Effect::Create { kind, name } => (*kind, name),
            Effect::Alter {
                kind: ObjectKind::Event,
                name,
            } => (ObjectKind::Event, name),
            _ => return Ok(()),
        };
        let Some(pool) = &self.ctl_pool else {
            return Ok(());
        };
        let (sql, keyword) = match kind {
            ObjectKind::Procedure => (
                "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? \
                 AND ROUTINE_NAME = ? AND ROUTINE_TYPE = 'PROCEDURE'",
                "PROCEDURE",
            ),
            ObjectKind::Function => (
                "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? \
                 AND ROUTINE_NAME = ? AND ROUTINE_TYPE = 'FUNCTION'",
                "FUNCTION",
            ),
            ObjectKind::Trigger => (
                "SELECT DEFINER FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? \
                 AND TRIGGER_NAME = ?",
                "TRIGGER",
            ),
            ObjectKind::Event => (
                "SELECT DEFINER FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ? \
                 AND EVENT_NAME = ?",
                "EVENT",
            ),
            _ => return Ok(()),
        };
        let schema = stmt.schema.to_string();
        let mut c = pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, "sprig_ctl unreachable").with_source(e)
        })?;
        let got: Option<String> = c
            .exec_first(sql, (&schema, name))
            .await
            .map_err(|e| SprigError::internal(format!("reading a definer: {e}")))?;
        match got {
            Some(d) if d != want => {
                let drop = format!(
                    "DROP {keyword} IF EXISTS {}.{}",
                    sprig_common::names::quote(&schema),
                    sprig_common::names::quote(name)
                );
                if let Err(e) = c.query_drop(drop).await {
                    tracing::error!(error = %e, schema, name, "dropping a stored program with a wrong definer failed");
                }
                tracing::warn!(schema, name, definer = %d, "stored program refused: wrong definer");
                Err(sprig_auth::authz::definers::mismatch(keyword, name, &d))
            }
            _ => Ok(()),
        }
    }
}

/// `` `x` `` with backticks doubled.
fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Every `DEFINER = <account>` clause in `sql` (also inside executable
/// comments) replaced by `with`, and how many there were. `CURRENT_USER`,
/// `CURRENT_USER()`, `CURRENT_ROLE`, a role and `'u'@'h'` in any quoting
/// are accounts.
pub(crate) fn replace_definers(sql: &[u8], lex: LexOptions, with: &[u8]) -> (Vec<u8>, usize) {
    let Ok(tokens) = sprig_sql::lex::lex(sql, &lex) else {
        return (sql.to_vec(), 0);
    };
    let t: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut out = Vec::with_capacity(sql.len());
    let mut copied = 0;
    let mut n = 0;
    let mut i = 0;
    while i < t.len() {
        let is_definer = t[i].kind == TokenKind::Word
            && t[i].bytes(sql).eq_ignore_ascii_case(b"DEFINER")
            && t.get(i + 1).is_some_and(|e| e.bytes(sql) == b"=");
        if !is_definer {
            i += 1;
            continue;
        }
        let Some(user) = t.get(i + 2) else { break };
        let mut last = i + 2;
        let current = user.kind == TokenKind::Word
            && (user.bytes(sql).eq_ignore_ascii_case(b"CURRENT_USER")
                || user.bytes(sql).eq_ignore_ascii_case(b"CURRENT_ROLE"));
        if current {
            if t.get(last + 1).is_some_and(|p| p.bytes(sql) == b"(")
                && t.get(last + 2).is_some_and(|p| p.bytes(sql) == b")")
            {
                last += 2;
            }
        } else if matches!(
            user.kind,
            TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
        ) {
            match t.get(last + 1) {
                // `'u'@'h'`, `` `u`@`h` ``: the host lexes as a user variable.
                Some(h) if h.kind == TokenKind::UserVar => last += 1,
                Some(at) if at.bytes(sql) == b"@" && t.get(last + 2).is_some() => last += 2,
                _ => {}
            }
        } else {
            i += 1;
            continue;
        }
        out.extend_from_slice(&sql[copied..t[i].start as usize]);
        out.extend_from_slice(with);
        copied = t[last].end as usize;
        n += 1;
        i = last + 1;
    }
    out.extend_from_slice(&sql[copied..]);
    (out, n)
}

/// A stored program's physical text with `definer` (`DEFINER=...`) as its
/// only definer: every clause replaced, or one added after `CREATE [OR
/// REPLACE]` (or `ALTER`, for `ALTER EVENT`, which otherwise makes the
/// running account the definer). Every executable comment counts, whatever
/// its version, so no clause MariaDB would run stays unseen.
fn with_definer(sql: &str, definer: &str, effect: &Effect) -> String {
    let lex = LexOptions::mariadb().version(999_999);
    let (out, n) = replace_definers(sql.as_bytes(), lex, definer.as_bytes());
    let add = n == 0
        && matches!(
            effect,
            Effect::Create { .. }
                | Effect::Alter {
                    kind: ObjectKind::Event,
                    ..
                }
        );
    let out = if add {
        insert_definer(&out, lex, definer).unwrap_or(out)
    } else {
        out
    };
    String::from_utf8_lossy(&out).into_owned()
}

/// `definer` added after the leading `CREATE [OR REPLACE]` or `ALTER`.
fn insert_definer(sql: &[u8], lex: LexOptions, definer: &str) -> Option<Vec<u8>> {
    let tokens = sprig_sql::lex::lex(sql, &lex).ok()?;
    let t: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let first = t.first()?;
    let word = |i: usize, w: &[u8]| {
        t.get(i)
            .is_some_and(|x| x.kind == TokenKind::Word && x.bytes(sql).eq_ignore_ascii_case(w))
    };
    let at = if word(0, b"CREATE") && word(1, b"OR") && word(2, b"REPLACE") {
        t[2].end
    } else if word(0, b"CREATE") || word(0, b"ALTER") {
        first.end
    } else {
        return None;
    } as usize;
    let mut out = Vec::with_capacity(sql.len() + definer.len() + 1);
    out.extend_from_slice(&sql[..at]);
    out.push(b' ');
    out.extend_from_slice(definer.as_bytes());
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// Routines, triggers and events: with binary logging on, MariaDB refuses
/// to create them (1419) for an account without `SUPER`, unless
/// `log_bin_trust_function_creators` is on server-wide. R0 runs them as
/// `sprig_ctl` in the object's schema (the engine's `PhysicalDdl::schema`),
/// where unqualified names in the statement resolve as in the session (the
/// object's database is the session's current one unless qualified, and
/// qualified names are already physical). Their `DEFINER` is the session's
/// own backend account, never `sprig_ctl` (which reaches every tenant's
/// schemas): see [`GatewayDdl::stored_program`].
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
        if stored_program(&stmt.effect) {
            return self.stored_program(stmt).await;
        }
        if stmt.effect.on_schema() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create(kind: ObjectKind) -> Effect {
        Effect::Create {
            kind,
            name: "p".into(),
        }
    }

    const D: &str = "DEFINER=`t1_u7`@`%`";

    #[test]
    fn stored_programs_get_the_session_account_as_definer() {
        let p = create(ObjectKind::Procedure);
        assert_eq!(
            with_definer("CREATE PROCEDURE p() SELECT 1", D, &p),
            "CREATE DEFINER=`t1_u7`@`%` PROCEDURE p() SELECT 1"
        );
        assert_eq!(
            with_definer("create or replace procedure p() select 1", D, &p),
            "create or replace DEFINER=`t1_u7`@`%` procedure p() select 1"
        );
        assert_eq!(
            with_definer("CREATE DEFINER=CURRENT_USER PROCEDURE p() SELECT 1", D, &p),
            "CREATE DEFINER=`t1_u7`@`%` PROCEDURE p() SELECT 1"
        );
        assert_eq!(
            with_definer(
                "CREATE DEFINER = 'root'@'localhost' FUNCTION f() RETURNS INT RETURN 1",
                D,
                &p
            ),
            "CREATE DEFINER=`t1_u7`@`%` FUNCTION f() RETURNS INT RETURN 1"
        );
        // mysqldump's trigger form, and a clause in a comment MariaDB 12 runs.
        assert_eq!(
            with_definer(
                "/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET @a = 1 */",
                D,
                &create(ObjectKind::Trigger)
            ),
            "/*!50003 CREATE*/ /*!50017 DEFINER=`t1_u7`@`%`*/ /*!50003 TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET @a = 1 */"
        );
        assert_eq!(
            with_definer(
                "CREATE /*M!120000 DEFINER=sprig_ctl*/ EVENT e ON SCHEDULE EVERY 1 HOUR DO SELECT 1",
                D,
                &create(ObjectKind::Event)
            ),
            "CREATE /*M!120000 DEFINER=`t1_u7`@`%`*/ EVENT e ON SCHEDULE EVERY 1 HOUR DO SELECT 1"
        );
        // A role as definer.
        assert_eq!(
            with_definer("CREATE DEFINER=CURRENT_ROLE PROCEDURE p() SELECT 1", D, &p),
            "CREATE DEFINER=`t1_u7`@`%` PROCEDURE p() SELECT 1"
        );
        // Strings in the body are left alone.
        assert_eq!(
            with_definer("CREATE PROCEDURE p() SELECT 'DEFINER=x'", D, &p),
            "CREATE DEFINER=`t1_u7`@`%` PROCEDURE p() SELECT 'DEFINER=x'"
        );
    }

    #[test]
    fn alter_event_keeps_the_session_account() {
        let alter = Effect::Alter {
            kind: ObjectKind::Event,
            name: "e".into(),
        };
        assert_eq!(
            with_definer("ALTER EVENT e COMMENT 'x'", D, &alter),
            "ALTER DEFINER=`t1_u7`@`%` EVENT e COMMENT 'x'"
        );
        // ALTER PROCEDURE has no definer clause and keeps the definer.
        let alter_p = Effect::Alter {
            kind: ObjectKind::Procedure,
            name: "p".into(),
        };
        assert_eq!(
            with_definer("ALTER PROCEDURE p COMMENT 'x'", D, &alter_p),
            "ALTER PROCEDURE p COMMENT 'x'"
        );
    }
}
