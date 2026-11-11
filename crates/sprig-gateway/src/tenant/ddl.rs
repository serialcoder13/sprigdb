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

use std::sync::Arc;

use sprig_catalog::ddl::{
    DdlExecutor, Effect, MariaExecutor, PhysicalDdl, PhysicalObject, SchemaInfo, ShardTarget,
};
use sprig_catalog::store::{DatabaseRow, ObjectKind};
use sprig_common::ids::{ClusterId, TenantId};
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
    /// The version string the session's persona reports: `VERSION()` in a
    /// body becomes it (gaps row 53).
    version: Option<String>,
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
            version: None,
        }
    }

    /// `VERSION()` in views and stored programs gives `v`, the version the
    /// session's persona reports (chapter 07 step 12; gaps row 53).
    #[must_use]
    pub fn reporting_version(mut self, v: String) -> Self {
        self.version = Some(v);
        self
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

    /// The `sql_mode` a stored program created by this session records
    /// (`None`: the server's default, which `sprig_ctl` runs too), and the
    /// lexer options of the session's text (chapter 28, R4; gaps row 27).
    async fn session_modes(&self) -> (Option<String>, LexOptions) {
        let bs = self.bs.lock().await;
        let vars = bs.vars();
        let backend = vars.vars.get("sql_mode").cloned();
        let stored = match &vars.client_sql_mode {
            // 3306: the client's view, with ONLY_FULL_GROUP_BY marked.
            Some(view) => sprig_sql::compat::sqlmode::parse_mysql(view)
                .ok()
                .map(sprig_sql::compat::sqlmode::Mode::stored_program)
                .or_else(|| backend.clone()),
            None => backend.clone(),
        };
        let has = |n: &str| {
            backend
                .as_deref()
                .is_some_and(|m| m.split(',').any(|i| i.trim().eq_ignore_ascii_case(n)))
        };
        let lex = LexOptions::mariadb()
            .ansi_quotes(has("ANSI_QUOTES") || has("ANSI"))
            .no_backslash_escapes(has("NO_BACKSLASH_ESCAPES"));
        (stored, lex)
    }

    /// A stored program: as `sprig_ctl`, with the session's account as
    /// `DEFINER` and the session's `sql_mode`, then the definer MariaDB
    /// recorded is checked.
    async fn stored_program(&self, stmt: &PhysicalDdl, mode: Option<&str>) -> Result<()> {
        let (user, host) = self.session_account().await?;
        let definer = format!("DEFINER={}@{}", bq(&user), bq(&host));
        let sql = in_mode(&with_definer(&stmt.sql, &definer, &stmt.effect), mode);
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
        match &self.ctl_pool {
            Some(pool) => verify_definer_on(pool, stmt, want).await,
            None => Ok(()),
        }
    }
}

/// After `CREATE` (and `ALTER EVENT`) through `pool` (`sprig_ctl` on the
/// object's cluster): MariaDB must have recorded `want` as the definer;
/// otherwise the object is dropped and the statement fails with 39367.
async fn verify_definer_on(pool: &mysql_async::Pool, stmt: &PhysicalDdl, want: &str) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let (kind, name) = match &stmt.effect {
        Effect::Create { kind, name } => (*kind, name),
        Effect::Alter {
            kind: ObjectKind::Event,
            name,
        } => (ObjectKind::Event, name),
        _ => return Ok(()),
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

/// `sql` run under `mode` (chapter 28, R4; gaps row 27): MariaDB records
/// the creating session's `sql_mode` with each routine, trigger and event,
/// as MySQL does, and parses the statement under it. `sprig_ctl`'s pooled
/// connection gets `SET SESSION sql_mode` first, in the same request; the
/// pool resets the connection when it comes back.
fn in_mode(sql: &str, mode: Option<&str>) -> String {
    match mode {
        Some(m) => format!(
            "SET SESSION sql_mode = {}; {sql}",
            sprig_sql::names::rewrite::sql_string(m, false)
        ),
        None => sql.to_owned(),
    }
}

/// Chapter 07 step 12: a `CREATE` or `ALTER` of a view, routine, trigger
/// or event with its account functions and `DATABASE()` turned into
/// lookups of logical names ([`sprig_sql::names::stored::substitute`]);
/// `None` when there is nothing to change.
/// `mode` is the `sql_mode` the body is created under: with `ORACLE`,
/// `SQLERRM` is mapped too.
fn names_in_body(
    stmt: &PhysicalDdl,
    lex: LexOptions,
    mode: Option<&str>,
    version: Option<&str>,
) -> Option<PhysicalDdl> {
    let kind = match &stmt.effect {
        Effect::Create { kind, .. } | Effect::Alter { kind, .. } => *kind,
        _ => return None,
    };
    if !matches!(
        kind,
        ObjectKind::View
            | ObjectKind::Procedure
            | ObjectKind::Function
            | ObjectKind::Trigger
            | ObjectKind::Event
    ) {
        return None;
    }
    let oracle = mode.is_some_and(|m| {
        m.split(',')
            .any(|i| i.trim().eq_ignore_ascii_case("ORACLE"))
    });
    let sql = sprig_sql::names::stored::substitute_full(&stmt.sql, &lex, oracle, version)?;
    Some(PhysicalDdl {
        sql,
        ..stmt.clone()
    })
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
        let (mode, lex) = self.session_modes().await;
        let sub = names_in_body(stmt, lex, mode.as_deref(), self.version.as_deref());
        let stmt = sub.as_ref().unwrap_or(stmt);
        if stored_program(&stmt.effect) {
            return self.stored_program(stmt, mode.as_deref()).await;
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

/// Chapter 21's wait for chapter 20's shard jobs (open question 10): a
/// split, merge or shard move holds `mover/shard-locks/<tenant>/g<g>s<s>`
/// in the tenant's home DC tier for each source shard, from admission
/// until the job ends. A schema change on such a shard polls until the
/// lock is gone, up to `wait` (`ddl.mover_wait_minutes`), then fails with
/// 39470. A locked shard that is no longer in its group's map belongs to
/// a job that already committed: the statement's layout is stale, so it
/// fails with 39110 and the client retries on the new map.
#[derive(Clone)]
pub struct MoverLocks {
    store: Arc<dyn sprig_meta::MetaStore>,
    wait: std::time::Duration,
    poll: std::time::Duration,
}

impl std::fmt::Debug for MoverLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoverLocks")
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

impl MoverLocks {
    /// Locks in `store` (the tenant's home DC tier), waited for at most
    /// `wait`.
    #[must_use]
    pub fn new(store: Arc<dyn sprig_meta::MetaStore>, wait: std::time::Duration) -> Self {
        Self {
            store,
            wait,
            poll: std::time::Duration::from_millis(500),
        }
    }

    /// The same with another polling interval (tests).
    #[must_use]
    pub fn with_poll(mut self, poll: std::time::Duration) -> Self {
        self.poll = poll;
        self
    }

    /// The job holding one of `shards`, if any.
    async fn holder(&self, tenant: TenantId, shards: &[SchemaName]) -> Result<Option<String>> {
        for s in shards {
            let SchemaName::Shard { group, shard, .. } = s else {
                continue;
            };
            let key = self
                .store
                .tier()
                .key(&format!("mover/shard-locks/{tenant}/g{group}s{shard}"))?;
            let Some(kv) = self
                .store
                .get(&key, sprig_meta::Consistency::Linearizable)
                .await?
            else {
                continue;
            };
            let job = String::from_utf8_lossy(&kv.value).into_owned();
            let (map, _) = sprig_shard::ShardMapStore::new(Arc::clone(&self.store))
                .load(tenant, *group)
                .await?;
            if map.shard(*shard).is_none() {
                return Err(SprigError::new(
                    SprigCode::ShardMoved,
                    format!(
                        "shard {shard} of table group {group} was replaced by job {job} \
                         while the statement was planned; retry it"
                    ),
                ));
            }
            return Ok(Some(format!("job {job} (group {group}, shard {shard})")));
        }
        Ok(None)
    }

    /// Waits until no split, merge or shard move holds one of `shards`.
    ///
    /// # Errors
    /// 39470 when `wait` runs out; 39110 for a stale layout; etcd errors.
    pub async fn wait(&self, tenant: TenantId, shards: &[SchemaName]) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.wait;
        let mut logged = false;
        loop {
            let h = match self.holder(tenant, shards).await {
                Ok(None) => return Ok(()),
                Ok(Some(h)) => h,
                // etcd slow or briefly unreachable: look again until the
                // deadline rather than fail the change.
                Err(e)
                    if e.retryable
                        && e.code() != Some(SprigCode::ShardMoved)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tracing::debug!(%tenant, error = %e, "shard locks unreadable; retrying");
                    tokio::time::sleep(self.poll).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if tokio::time::Instant::now() >= deadline {
                metrics::counter!("sprig_ddl_mover_waits_total", "outcome" => "timeout")
                    .increment(1);
                return Err(SprigError::new(
                    SprigCode::ShardBusy,
                    format!(
                        "a split, merge or shard move holds a shard of this table: {h}; \
                         waited {} s (ddl.mover_wait_minutes); retry when it finishes",
                        self.wait.as_secs()
                    ),
                ));
            }
            if !logged {
                logged = true;
                metrics::counter!("sprig_ddl_mover_waits_total", "outcome" => "waited")
                    .increment(1);
                tracing::info!(%tenant, holder = %h, "schema change waits for a shard job");
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// Where a database's shards are: for its template schema and a table
/// (`None`: routines, events and views), the shard schemas and their
/// clusters, or `None` when the statement stays on the template (R0).
pub type ShardLayout =
    Arc<dyn Fn(&str, Option<&str>) -> Option<Vec<(String, ClusterId)>> + Send + Sync>;

/// Chapter 21 in the gateway: DDL on a table group's shards.
///
/// The catalog's [`DdlEngine`](sprig_catalog::ddl::DdlEngine) asks
/// [`DdlExecutor::shards`] where a statement goes; this executor answers
/// from `layout` (in R1 the router's topology) and runs each shard's
/// statement as `sprig_ctl` on that shard's cluster, after the gateway's
/// own privilege check. Stored programs keep chapter 29's rule on every
/// shard: their definer is the creating user's backend account
/// (`t<tenant>_u<user>`@`%`), written into the statement and checked after
/// creation. Statements on schemas outside the layout (unsharded tables,
/// the template) go to the session's [`GatewayDdl`] as before.
pub struct ShardedDdl<'a> {
    inner: GatewayDdl<'a>,
    layout: ShardLayout,
    movers: Option<MoverLocks>,
    locator: Arc<super::locator::GatewayLocator>,
    definer: Option<(String, String)>,
    routes:
        std::sync::Mutex<std::collections::BTreeMap<SchemaName, (ClusterId, mysql_async::Pool)>>,
}

impl std::fmt::Debug for ShardedDdl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedDdl").finish_non_exhaustive()
    }
}

impl<'a> ShardedDdl<'a> {
    /// `inner` for the session's schemas; shards from `layout`, reached
    /// through `locator`'s `sprig_ctl` pools; stored programs defined by
    /// `user`'s backend account.
    #[must_use]
    pub fn new(
        inner: GatewayDdl<'a>,
        layout: ShardLayout,
        locator: Arc<super::locator::GatewayLocator>,
        tenant: TenantId,
        user: Option<sprig_common::ids::UserId>,
    ) -> Self {
        Self {
            inner,
            layout,
            movers: None,
            locator,
            definer: user.map(|u| {
                (
                    sprig_auth::authz::backend::account(tenant, u),
                    "%".to_owned(),
                )
            }),
            routes: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    /// The same executor waiting for chapter 20's shard jobs (see
    /// [`MoverLocks`]).
    #[must_use]
    pub fn with_mover_locks(mut self, movers: MoverLocks) -> Self {
        self.movers = Some(movers);
        self
    }

    fn route(&self, schema: &SchemaName) -> Option<(ClusterId, mysql_async::Pool)> {
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(schema)
            .cloned()
    }

    fn ctl(&self, schema: &SchemaName) -> Option<MariaExecutor> {
        self.route(schema).map(|(_, p)| MariaExecutor::new(p))
    }

    async fn stored_program_on(
        &self,
        pool: &mysql_async::Pool,
        stmt: &PhysicalDdl,
        mode: Option<&str>,
    ) -> Result<()> {
        let Some((user, host)) = self.definer.clone() else {
            return Err(SprigError::internal(
                "a stored program on several shards needs the creating user",
            ));
        };
        let definer = format!("DEFINER={}@{}", bq(&user), bq(&host));
        let st = PhysicalDdl {
            sql: in_mode(&with_definer(&stmt.sql, &definer, &stmt.effect), mode),
            ..stmt.clone()
        };
        MariaExecutor::new(pool.clone()).execute(&st).await?;
        verify_definer_on(pool, stmt, &format!("{user}@{host}")).await
    }
}

#[async_trait::async_trait]
impl DdlExecutor for ShardedDdl<'_> {
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        let Some((_, pool)) = self.route(&stmt.schema) else {
            return self.inner.execute(stmt).await;
        };
        let (mode, lex) = self.inner.session_modes().await;
        let sub = names_in_body(stmt, lex, mode.as_deref(), self.inner.version.as_deref());
        let stmt = sub.as_ref().unwrap_or(stmt);
        if stored_program(&stmt.effect) && !matches!(stmt.effect, Effect::Drop { .. }) {
            return self.stored_program_on(&pool, stmt, mode.as_deref()).await;
        }
        // Undo re-creations (`Effect::Restore`) keep the definer they had.
        MariaExecutor::new(pool).execute(stmt).await
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        match self.ctl(schema) {
            Some(e) => e.schema(schema).await,
            None => self.inner.schema(schema).await,
        }
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        match self.ctl(schema) {
            Some(e) => e.show_create(schema, kind, name).await,
            None => self.inner.show_create(schema, kind, name).await,
        }
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        match self.ctl(schema) {
            Some(e) => e.objects(schema).await,
            None => self.inner.objects(schema).await,
        }
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        self.inner.tenant_schemas(tenant).await
    }

    async fn busy(&self, schema: &SchemaName) -> Result<bool> {
        match self.ctl(schema) {
            Some(e) => e.busy(schema).await,
            None => self.inner.busy(schema).await,
        }
    }

    async fn wait_for_shard_jobs(&self, tenant: TenantId, shards: &[SchemaName]) -> Result<()> {
        match &self.movers {
            Some(m) => m.wait(tenant, shards).await,
            None => Ok(()),
        }
    }

    async fn shards(
        &self,
        tenant: TenantId,
        db: &DatabaseRow,
        target: ShardTarget<'_>,
    ) -> Result<Vec<SchemaName>> {
        let default = sprig_catalog::ddl::shard_schemas(tenant, db);
        let Some(template) = default.first() else {
            return Ok(default);
        };
        let table = match target {
            ShardTarget::Table(t) => Some(t),
            ShardTarget::Database => None,
        };
        let Some(found) = (self.layout)(&template.to_string(), table) else {
            return Ok(default);
        };
        if found.is_empty() {
            return Ok(default);
        }
        let mut out = Vec::new();
        for (schema, cluster) in found {
            let s: SchemaName = schema
                .parse()
                .map_err(|e| SprigError::internal(format!("a shard schema in the layout: {e}")))?;
            if s.db() != Some(db.db_id) {
                return Ok(default);
            }
            if self.route(&s).is_none() {
                let pool = self.locator.ctl_pool(&cluster).await?;
                self.routes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(s, (cluster, pool));
            }
            out.push(s);
        }
        Ok(out)
    }

    fn cluster_of(&self, schema: &SchemaName) -> Option<ClusterId> {
        self.route(schema).map(|(c, _)| c)
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
    #[allow(clippy::too_many_lines)] // one case per kind of stored code
    fn stored_code_runs_in_the_session_mode_with_logical_names() {
        assert_eq!(
            in_mode("CREATE PROCEDURE p() SELECT 1", Some("PIPES_AS_CONCAT")),
            "SET SESSION sql_mode = 'PIPES_AS_CONCAT'; CREATE PROCEDURE p() SELECT 1"
        );
        assert_eq!(
            in_mode("CREATE VIEW v AS SELECT 1", None),
            "CREATE VIEW v AS SELECT 1"
        );
        let schema: SchemaName = "t1_d1_g1_s1".parse().unwrap();
        let stmt = |sql: &str, effect: Effect| PhysicalDdl {
            schema,
            sql: sql.to_owned(),
            effect,
        };
        let lex = LexOptions::mariadb();
        let v = names_in_body(
            &stmt(
                "CREATE VIEW v AS SELECT CURRENT_USER() AS u",
                create(ObjectKind::View),
            ),
            lex,
            None,
            None,
        )
        .unwrap();
        assert!(
            v.sql.contains("`__sprig_sys`.`__sprig_names_v`"),
            "{}",
            v.sql
        );
        let p = names_in_body(
            &stmt(
                "CREATE PROCEDURE p() SELECT DATABASE() INTO @d",
                create(ObjectKind::Procedure),
            ),
            lex,
            None,
            None,
        )
        .unwrap();
        assert!(p.sql.contains("WHERE `name` = DATABASE())"), "{}", p.sql);
        // Tables and drops are left alone.
        assert!(
            names_in_body(
                &stmt(
                    "CREATE TABLE t (u VARCHAR(80) DEFAULT (CURRENT_USER()))",
                    create(ObjectKind::Table)
                ),
                lex,
                None,
                None
            )
            .is_none()
        );
        assert!(
            names_in_body(
                &stmt(
                    "DROP VIEW v",
                    Effect::Drop {
                        kind: ObjectKind::View,
                        names: vec!["v".into()]
                    }
                ),
                lex,
                None,
                None
            )
            .is_none()
        );
        // Caught error text (`GET DIAGNOSTICS`, and `SQLERRM` under ORACLE).
        let f = names_in_body(
            &stmt(
                "CREATE FUNCTION f() RETURNS TEXT BEGIN DECLARE m TEXT; DECLARE CONTINUE HANDLER \
                 FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 m = MESSAGE_TEXT; RETURN m; END",
                create(ObjectKind::Function),
            ),
            lex,
            None,
            None,
        )
        .unwrap();
        assert!(
            f.sql
                .contains("SET m = `__sprig_sys`.`__sprig_names_map`(m); END"),
            "{}",
            f.sql
        );
        let body = "CREATE PROCEDURE p AS BEGIN SELECT 1; EXCEPTION WHEN OTHERS THEN SET @e = SQLERRM; END";
        let o = names_in_body(
            &stmt(body, create(ObjectKind::Procedure)),
            lex,
            Some("PIPES_AS_CONCAT,ANSI_QUOTES,ORACLE"),
            None,
        )
        .unwrap();
        assert!(
            o.sql
                .contains("SET @e = `__sprig_sys`.`__sprig_names_map`(SQLERRM)"),
            "{}",
            o.sql
        );
        assert!(
            names_in_body(&stmt(body, create(ObjectKind::Procedure)), lex, None, None).is_none()
        );
    }

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

#[cfg(test)]
mod mover_lock_tests {
    #![allow(clippy::unwrap_used)]
    use std::time::Duration;

    use sprig_common::ids::{DbId, DcId, GroupId, ShardId};
    use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

    use super::*;

    const T: TenantId = TenantId(3_930_600_001);

    async fn put(s: &Arc<dyn MetaStore>, rel: &str, v: &[u8]) {
        let key = s.tier().key(rel).unwrap();
        s.txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: v.to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    }

    async fn del(s: &Arc<dyn MetaStore>, rel: &str) {
        let key = s.tier().key(rel).unwrap();
        s.txn(Txn {
            when: vec![],
            then: vec![Op::Delete(key)],
            otherwise: vec![],
        })
        .await
        .unwrap();
    }

    fn shard(s: u32) -> SchemaName {
        SchemaName::shard(T, DbId(1), GroupId(3), ShardId(s))
    }

    /// Chapter 21 open question 10: a schema change waits for a split of
    /// its shards, gives up with 39470, and calls a replaced shard stale.
    #[tokio::test]
    async fn schema_changes_wait_for_shard_jobs() {
        let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-a").unwrap())));
        let map = sprig_shard::ShardMap::presplit(
            T,
            GroupId(3),
            0,
            2,
            &[ClusterId::new("dc-a-c1").unwrap()],
        )
        .unwrap();
        sprig_shard::ShardMapStore::new(Arc::clone(&s))
            .create(&map)
            .await
            .unwrap();
        let locks = MoverLocks::new(Arc::clone(&s), Duration::from_millis(60))
            .with_poll(Duration::from_millis(5));
        // Nothing holds the shards.
        locks.wait(T, &[shard(1), shard(2)]).await.unwrap();
        // A split of shard 2 runs: refused when the wait runs out.
        let lock = format!("mover/shard-locks/{T}/g3s2");
        put(&s, &lock, b"job-a").await;
        let e = locks.wait(T, &[shard(1), shard(2)]).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ShardBusy));
        assert!(e.message.contains("job-a"), "{}", e.message);
        // Other shards don't wait.
        locks.wait(T, &[shard(1)]).await.unwrap();
        // The split ends while a change waits: the change goes ahead.
        let waiting = {
            let l = locks.clone().with_poll(Duration::from_millis(5));
            let l = MoverLocks {
                wait: Duration::from_secs(5),
                ..l
            };
            tokio::spawn(async move { l.wait(T, &[shard(2)]).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        del(&s, &lock).await;
        waiting.await.unwrap().unwrap();
        // A lock on a shard no longer in the map: the job committed and
        // the statement's layout is stale.
        put(&s, &format!("mover/shard-locks/{T}/g3s7"), b"job-b").await;
        let e = locks.wait(T, &[shard(7)]).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ShardMoved));
        // ...at once, however long the change may wait.
        let patient = MoverLocks::new(Arc::clone(&s), Duration::from_secs(30))
            .with_poll(Duration::from_millis(5));
        let t0 = std::time::Instant::now();
        let e = patient.wait(T, &[shard(7)]).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ShardMoved));
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    }
}
