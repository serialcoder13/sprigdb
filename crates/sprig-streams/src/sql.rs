//! Streams in a tenant session: the gateway calls [`handle`] for each
//! statement before the name plan, with a [`Host`] that gives it the catalog snapshot, privileges,
//! shard schemas and `sprig_ctl` connections.
//!
//! ```text
//! CREATE STREAM s ...:
//!   privilege: CREATE on the database
//!   outbox:  the attached table's shard (ATTACHED TO GROUP OF t; its group
//!            must have one shard: 39945 otherwise), or the database's
//!            default group; log: the database's default group (one log shard)
//!   sprig_ctl on each shard: the outbox, the log, the checkpoint rows with
//!     the cluster's binlog position now (the sequencer's start)
//!   one versioned catalog change: the streams row (and stream.retention_hours
//!     at the stream's scope); the feed subscription stream:<id> for the
//!     controller, which starts the sequencer job
//! INSERT INTO s (key, value, headers) ...:
//!   INSERT on the stream (by name, or the database); literals above
//!   stream.max_message_bytes: 39944; ON DUPLICATE KEY: 39941; change topic: 39941
//!   → INSERT INTO `__sprig_eo_<id>` (k, v, headers) ... through the session,
//!     inside its transaction
//! SELECT ... FROM s: SELECT on the stream; a change topic: SELECT on its
//!   source table, and 39366 when the authorization engine filters that
//!   table's rows (row-level security isn't applied to streams yet)
//!   → derived tables over the logs ([`crate::read`]), through the session
//! UPDATE | DELETE | REPLACE | TRUNCATE s: 39941
//! ```
//!
//! Change topics: after `ALTER TABLE t SPRIG SET (stream.change_topic =
//! on | off)` succeeds, the gateway calls [`reconcile_change_topic`], which
//! creates `t.changes` (or drops it) to match the table's effective
//! setting.

use std::collections::BTreeMap;

use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::LexOptions;

use crate::ddl;
use crate::grammar::{self, InsertInto, QName, Stmt};
use crate::model::{
    Cleanup, Envelope, LogShard, PartitionNo, ShardSlot, StreamDef, StreamId, StreamKind,
    StreamState, StreamsState, change_topic_name, valid_name,
};
use crate::read;
use crate::sequencer::Start;

/// A privilege the stream layer checks (the authorization model's names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Privilege {
    /// Read.
    Select,
    /// Publish.
    Insert,
    /// Create a stream.
    Create,
    /// Change one.
    Alter,
    /// Drop one.
    Drop,
}

impl Privilege {
    /// The command in MariaDB's "command denied" text.
    #[must_use]
    pub const fn command(self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Create => "CREATE",
            Privilege::Alter => "ALTER",
            Privilege::Drop => "DROP",
        }
    }
}

/// A shard schema with its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysSlot {
    /// The slot.
    pub slot: ShardSlot,
    /// Its physical schema.
    pub schema: String,
    /// Its cluster now.
    pub cluster: ClusterId,
}

/// A table, as the stream layer needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    /// `objects.object_id`.
    pub object: ObjectId,
    /// Its logical name as stored.
    pub name: String,
    /// The shard schemas of its group.
    pub slots: Vec<PhysSlot>,
}

/// Settings the statements read, resolved for one database (and object).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StmtSettings {
    /// `stream.partitions_default`.
    pub partitions_default: u32,
    /// `stream.max_message_bytes`.
    pub max_message_bytes: u64,
}

/// What a catalog change writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsChange {
    /// A new stream, and its `stream.retention_hours` at its own scope.
    Insert(StreamDef, Option<u64>),
    /// A changed stream, and settings set (`Some`) or reset (`None`) at its
    /// own scope (`RETENTION` is `stream.retention_hours`).
    Update(StreamDef, Vec<crate::settings::SettingChange>),
    /// A dropped stream.
    Delete(StreamDef),
}

/// What the stream layer needs from the gateway.
#[async_trait::async_trait]
pub trait Host: Send {
    /// The tenant's id.
    fn tenant(&self) -> sprig_common::ids::TenantId;
    /// A logical database's id.
    fn db_id(&self, name: &str) -> Option<DbId>;
    /// A table of `db`.
    fn table(&self, db: DbId, name: &str) -> Option<TableInfo>;
    /// The database's default group's first shard (its template schema).
    fn template(&self, db: DbId) -> Option<PhysSlot>;
    /// Whether the session may do `p` on `name` of `db_name` (by object
    /// when `object` is given, else by name or on the database).
    fn allowed(&self, p: Privilege, db_name: &str, object: Option<ObjectId>, name: &str) -> bool;
    /// Whether the authorization engine filters `object`'s rows for reads.
    fn row_filtered(&self, db_name: &str, object: ObjectId) -> bool;
    /// Makes change topic `def`'s filter
    /// view (`__sprig_scf_<id>`, in its log schema) current, for a view or
    /// stored code that reads the topic. By default 39366: such
    /// definitions are refused.
    ///
    /// # Errors
    /// 39366; the backend's.
    async fn change_view(&mut self, def: &StreamDef) -> Result<()> {
        Err(SprigError::new(
            SprigCode::RlsUnsupported,
            format!(
                "ER_SPRIG_RLS_UNSUPPORTED: views and stored code can't read change topic '{}' \
                 here: its readers would get the table's rows without its row-level security",
                def.name
            ),
        ))
    }
    /// How change topic `def`'s records are
    /// filtered for this session when [`Host::row_filtered`] says its
    /// table's rows are: `None` when the session may read them all after
    /// all. By default 39366: a host that can't render the session's
    /// policies refuses the topic.
    ///
    /// # Errors
    /// 39366; the backend's.
    async fn change_filter(
        &mut self,
        db_name: &str,
        def: &StreamDef,
    ) -> Result<Option<read::RecordFilter>> {
        let _ = db_name;
        Err(SprigError::new(
            SprigCode::RlsUnsupported,
            format!(
                "ER_SPRIG_RLS_UNSUPPORTED: the authorization engine filters rows of '{}', which \
                 change topic '{}' can't apply here",
                def.source_name.as_deref().unwrap_or_default(),
                def.name
            ),
        ))
    }
    /// Settings for statements on `db`.
    fn settings(&self, db: DbId) -> StmtSettings;
    /// Whether `stream.change_topic` is on for table `object` of `db`.
    fn change_topic_on(&self, db: DbId, object: ObjectId) -> bool;
    /// The session's user, for "command denied" errors.
    fn user(&self) -> (String, String);

    /// Runs physical DDL on `slot` as `sprig_ctl`.
    ///
    /// # Errors
    /// The backend's.
    async fn ddl(&mut self, slot: &PhysSlot, statements: &[String]) -> Result<()>;
    /// `@@gtid_binlog_pos` of `slot`'s cluster.
    ///
    /// # Errors
    /// The backend's.
    async fn position(&mut self, slot: &PhysSlot) -> Result<String>;
    /// A fresh id from the tenant's object counter.
    ///
    /// # Errors
    /// The catalog's.
    async fn next_id(&mut self) -> Result<u64>;
    /// Commits `change` in one versioned catalog change (migrating the
    /// module first if needed).
    ///
    /// # Errors
    /// The catalog's (39942 when the name was taken meanwhile).
    async fn commit(&mut self, change: &StreamsChange) -> Result<()>;
    /// Registers the stream's feed subscription (the controller starts its
    /// sequencer).
    ///
    /// # Errors
    /// The DC tier's.
    async fn subscribe(&mut self, def: &StreamDef) -> Result<()>;
    /// Removes it.
    ///
    /// # Errors
    /// The DC tier's.
    async fn unsubscribe(&mut self, def: &StreamDef) -> Result<()>;
    /// The tenant's running sequencers' lag in ms, by stream (`SHOW
    /// STREAMS`' `Lag_ms`). By default none: the column is
    /// `NULL`.
    async fn lags(&mut self) -> std::collections::HashMap<StreamId, u64> {
        std::collections::HashMap::new()
    }
}

/// The statement's context.
#[derive(Debug, Clone, Copy)]
pub struct Ctx<'a> {
    /// The tenant's streams at the session's snapshot.
    pub state: &'a StreamsState,
    /// Lexing options.
    pub lex: LexOptions,
    /// `lower_case_table_names`.
    pub lctn: bool,
    /// The current logical database.
    pub current: Option<&'a str>,
    /// The persona is MySQL (affects nothing yet; kept for messages).
    pub mysql: bool,
}

/// What the gateway does with a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handled {
    /// Not the stream layer's: the statement goes on unchanged.
    NotStream,
    /// Done: answer OK.
    Ok {
        /// Affected rows.
        affected: u64,
    },
    /// A result set (text).
    Rows {
        /// Column names.
        columns: Vec<String>,
        /// Rows.
        rows: Vec<Vec<Option<String>>>,
    },
    /// Run this statement instead (logical names, internal mode, through
    /// the session).
    Forward {
        /// The statement.
        sql: String,
        /// Its kind (to map errors).
        write: bool,
        /// For a publish to a compacted stream: 4025 means a keyless
        /// record (39955).
        compact: bool,
    },
}

fn not_found(db: &str, name: &str) -> SprigError {
    SprigError::new(
        SprigCode::StreamNotFound,
        format!("ER_SPRIG_STREAM_NOT_FOUND: unknown stream '{db}.{name}'"),
    )
}

fn no_db() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn unknown_db(name: &str) -> SprigError {
    SprigError::standard(1049, *b"42000", format!("Unknown database '{name}'"))
}

fn denied(host: &dyn Host, p: Privilege, name: &str) -> SprigError {
    let (user, h) = host.user();
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{} command denied to user '{user}'@'{h}' for table '{name}'",
            p.command()
        ),
    )
}

fn append_only(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::StreamAppendOnly,
        format!("ER_SPRIG_STREAM_APPEND_ONLY: {what}"),
    )
}

/// The database a name is in.
fn db_of<'n>(q: &'n QName, cx: &'n Ctx<'_>) -> Result<&'n str> {
    q.db.as_deref().or(cx.current).ok_or_else(no_db)
}

/// The stream a name names, if any.
fn stream_of(
    cx: &Ctx<'_>,
    host: &dyn Host,
    q: &QName,
) -> Option<(String, std::sync::Arc<StreamDef>)> {
    let db_name = q.db.as_deref().or(cx.current)?;
    let db = host.db_id(db_name)?;
    let s = cx.state.get(db, &q.name, cx.lctn)?;
    (s.state == StreamState::Active).then(|| (db_name.to_owned(), std::sync::Arc::clone(s)))
}

/// Handles one statement of a tenant session.
///
/// # Errors
/// The stream errors of the module docs, 1064 for a malformed stream statement, and the
/// host's.
pub async fn handle(sql: &str, cx: &Ctx<'_>, host: &mut dyn Host) -> Result<Handled> {
    if !grammar::maybe_stream(sql.as_bytes(), !cx.state.is_empty()) {
        return Ok(Handled::NotStream);
    }
    match grammar::classify(sql.as_bytes(), &cx.lex)? {
        Stmt::Create(c) => create(&c, cx, host).await,
        Stmt::Alter(a) => alter(&a, cx, host).await,
        Stmt::Drop { if_exists, name } => drop_stream(&name, if_exists, cx, host).await,
        Stmt::Show { db } => show(db.as_deref(), cx, host).await,
        Stmt::Insert(i) => match stream_of(cx, host, &i.target) {
            Some((db, s)) => publish(sql, &i, &db, &s, cx, host),
            None => Ok(Handled::NotStream),
        },
        Stmt::Write { verb, target } => match stream_of(cx, host, &target) {
            Some((_, s)) => Err(append_only(&format!(
                "{verb} on stream '{}': a stream is append-only",
                s.name
            ))),
            None => Ok(Handled::NotStream),
        },
        Stmt::CreateTable(n) => match stream_of(cx, host, &n) {
            Some((db, s)) => Err(SprigError::new(
                SprigCode::StreamExists,
                format!("ER_SPRIG_STREAM_EXISTS: '{db}.{}' is a stream", s.name),
            )),
            None => Ok(Handled::NotStream),
        },
        Stmt::DropDatabase(name) => {
            // The database's streams go first (their tables go with its
            // schemas); then the statement runs as usual.
            if let Some(db) = host.db_id(&name) {
                let streams: Vec<StreamDef> = cx.state.of_db(db).map(|s| (**s).clone()).collect();
                if !streams.is_empty() && !host.allowed(Privilege::Drop, &name, None, "") {
                    return Ok(Handled::NotStream);
                }
                for s in &streams {
                    host.commit(&StreamsChange::Delete(s.clone())).await?;
                    host.unsubscribe(s).await?;
                }
            }
            Ok(Handled::NotStream)
        }
        Stmt::Other => select(sql, cx, host).await,
    }
}

/// The checkpoint rows and tables of a new log shard.
fn log_statements(slot: &PhysSlot, id: StreamId, partitions: u32, start: &Start) -> Vec<String> {
    let parts: Vec<PartitionNo> = (0..partitions).map(PartitionNo).collect();
    let today = ddl::day_of_ms(crate::maria::wall_ms());
    vec![
        ddl::log_sql(&slot.schema, id, today),
        ddl::ckpt_sql(&slot.schema),
        ddl::ckpt_rows_sql(&slot.schema, id, &parts, &start.encode()),
    ]
}

#[allow(clippy::too_many_lines)]
async fn create(c: &grammar::CreateStream, cx: &Ctx<'_>, host: &mut dyn Host) -> Result<Handled> {
    let Some(name) = &c.name else {
        return Err(SprigError::internal("CREATE STREAM without a name"));
    };
    let db_name = db_of(name, cx)?.to_owned();
    let db = host.db_id(&db_name).ok_or_else(|| unknown_db(&db_name))?;
    if !valid_name(&name.name) {
        return Err(SprigError::standard(
            1103,
            *b"42000",
            format!("Incorrect stream name '{}'", name.name),
        ));
    }
    if !host.allowed(Privilege::Create, &db_name, None, &name.name) {
        return Err(denied(host, Privilege::Create, &name.name));
    }
    if cx.state.get(db, &name.name, cx.lctn).is_some() || host.table(db, &name.name).is_some() {
        if c.if_not_exists {
            return Ok(Handled::Ok { affected: 0 });
        }
        return Err(SprigError::new(
            SprigCode::StreamExists,
            format!("ER_SPRIG_STREAM_EXISTS: '{db_name}.{}' exists", name.name),
        ));
    }
    let settings = host.settings(db);
    let partitions = c.partitions.unwrap_or(settings.partitions_default).max(1);
    let template = host
        .template(db)
        .ok_or_else(|| SprigError::internal(format!("database {db_name} has no shard")))?;
    // The outbox: the attached table's shard, or the default group's.
    let (attached_group, outbox_slot) = match &c.attached {
        Some(t) => {
            let tdb = t.db.as_deref().unwrap_or(&db_name);
            if tdb != db_name {
                return Err(SprigError::new(
                    SprigCode::StreamKeyNotAttached,
                    "ER_SPRIG_STREAM_KEY_NOT_ATTACHED: a stream attaches to a table of its own database",
                ));
            }
            let info = host.table(db, &t.name).ok_or_else(|| {
                SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{db_name}.{}' doesn't exist", t.name),
                )
            })?;
            if info.slots.len() != 1 {
                return Err(SprigError::new(
                    SprigCode::StreamKeyNotAttached,
                    format!(
                        "ER_SPRIG_STREAM_KEY_NOT_ATTACHED: table '{}' has {} shards; streams attach \
                         to one-shard groups in this release",
                        t.name,
                        info.slots.len()
                    ),
                ));
            }
            let slot = info.slots[0].clone();
            (Some(slot.slot.group), slot)
        }
        None => (None, template.clone()),
    };
    let id = StreamId(host.next_id().await?);
    let pos = host.position(&template).await?;
    let start = Start::At {
        cluster: template.cluster.clone(),
        pos: pos.parse()?,
    };
    let cleanup = c.cleanup.unwrap_or(Cleanup::Delete);
    let def = StreamDef {
        id,
        db,
        name: name.name.clone(),
        kind: StreamKind::Events,
        source_object: None,
        source_name: None,
        partitions,
        key_spec: c.key.clone(),
        attached_group,
        log_group: template.slot.group,
        cleanup,
        envelope: Envelope::Raw,
        state: StreamState::Active,
        outbox: vec![outbox_slot.slot],
        log: vec![LogShard {
            slot: template.slot,
            partitions: Vec::new(),
        }],
        created_at_ms: u64::try_from(crate::maria::wall_ms()).unwrap_or(0),
    };
    host.ddl(
        &outbox_slot,
        &[
            ddl::outbox_sql(&outbox_slot.schema, id, cleanup),
            crate::publish::producer_table_sql(&outbox_slot.schema, id),
        ],
    )
    .await?;
    host.ddl(
        &template,
        &log_statements(&template, id, partitions, &start),
    )
    .await?;
    if let Err(e) = host
        .commit(&StreamsChange::Insert(def.clone(), c.retention_hours))
        .await
    {
        let _ = host
            .ddl(
                &outbox_slot,
                &[
                    ddl::drop_outbox_sql(&outbox_slot.schema, id),
                    crate::publish::drop_producer_table_sql(&outbox_slot.schema, id),
                ],
            )
            .await;
        let _ = host
            .ddl(&template, &ddl::drop_log_sql(&template.schema, id))
            .await;
        return Err(e);
    }
    host.subscribe(&def).await?;
    tracing::info!(tenant = %host.tenant(), stream = id.0, name = %def.name, partitions, "stream created");
    Ok(Handled::Ok { affected: 0 })
}

async fn alter(a: &grammar::AlterStream, cx: &Ctx<'_>, host: &mut dyn Host) -> Result<Handled> {
    let db_name = db_of(&a.name, cx)?.to_owned();
    let (_, s) = stream_of(cx, host, &a.name).ok_or_else(|| not_found(&db_name, &a.name.name))?;
    if !host.allowed(Privilege::Alter, &db_name, None, &s.name) {
        return Err(denied(host, Privilege::Alter, &s.name));
    }
    let mut changes: Vec<crate::settings::SettingChange> = a
        .retention_hours
        .map(|h| crate::settings::SettingChange {
            key: crate::settings::RETENTION_HOURS.key,
            value: Some(h),
        })
        .into_iter()
        .collect();
    for (k, v) in &a.settings {
        let c = crate::settings::stream_setting(k, v.as_deref()).map_err(|m| {
            SprigError::standard(1231, *b"42000", format!("ALTER STREAM '{}': {m}", s.name))
        })?;
        changes.push(c);
    }
    let mut def = (*s).clone();
    if let Some(n) = a.partitions {
        if n < def.partitions {
            return Err(SprigError::new(
                SprigCode::StreamPartitionsDecrease,
                format!(
                    "ER_SPRIG_STREAM_PARTITIONS_DECREASE: stream '{}' has {} partitions; partitions can only grow",
                    def.name, def.partitions
                ),
            ));
        }
        if n > def.partitions {
            let slot = host
                .template(def.db)
                .ok_or_else(|| SprigError::internal("no shard"))?;
            if let Some(sql) = ddl::add_partitions_sql(&slot.schema, def.id, def.partitions, n) {
                host.ddl(&slot, &[sql]).await?;
            }
            def.partitions = n;
        }
    }
    if let Some(c) = a.cleanup
        && c != def.cleanup
    {
        if def.kind == StreamKind::Events {
            let slot = host
                .template(def.db)
                .ok_or_else(|| SprigError::internal("no shard"))?;
            let t = format!(
                "{}.{}",
                sprig_common::names::quote(&slot.schema),
                sprig_common::names::quote(&def.outbox_table())
            );
            let sql = if c == Cleanup::Compact {
                format!("ALTER TABLE {t} ADD CONSTRAINT sprig_sc_needs_key CHECK (k IS NOT NULL)")
            } else {
                format!("ALTER TABLE {t} DROP CONSTRAINT IF EXISTS sprig_sc_needs_key")
            };
            host.ddl(&slot, &[sql]).await?;
        }
        def.cleanup = c;
    }
    host.commit(&StreamsChange::Update(def, changes)).await?;
    Ok(Handled::Ok { affected: 0 })
}

/// Removes a stream: its catalog row first (clients stop seeing it), then
/// its subscription (the sequencer stops), then its tables.
async fn remove(def: &StreamDef, host: &mut dyn Host) -> Result<()> {
    host.commit(&StreamsChange::Delete(def.clone())).await?;
    host.unsubscribe(def).await?;
    let tenant = host.tenant();
    let Some(t) = host.template(def.db) else {
        return Ok(());
    };
    let phys = |slot: ShardSlot| PhysSlot {
        slot,
        schema: slot.schema(tenant, def.db).to_string(),
        cluster: t.cluster.clone(),
    };
    for slot in &def.outbox {
        let p = phys(*slot);
        let _ = host
            .ddl(
                &p,
                &[
                    ddl::drop_outbox_sql(&p.schema, def.id),
                    crate::publish::drop_producer_table_sql(&p.schema, def.id),
                ],
            )
            .await;
    }
    for l in &def.log {
        let p = phys(l.slot);
        let _ = host.ddl(&p, &ddl::drop_log_sql(&p.schema, def.id)).await;
    }
    Ok(())
}

async fn drop_stream(
    name: &QName,
    if_exists: bool,
    cx: &Ctx<'_>,
    host: &mut dyn Host,
) -> Result<Handled> {
    let db_name = db_of(name, cx)?.to_owned();
    let Some((_, s)) = stream_of(cx, host, name) else {
        if if_exists {
            return Ok(Handled::Ok { affected: 0 });
        }
        return Err(not_found(&db_name, &name.name));
    };
    if !host.allowed(Privilege::Drop, &db_name, None, &s.name) {
        return Err(denied(host, Privilege::Drop, &s.name));
    }
    remove(&s, host).await?;
    tracing::info!(tenant = %host.tenant(), stream = s.id.0, name = %s.name, "stream dropped");
    Ok(Handled::Ok { affected: 0 })
}

/// `SHOW STREAMS`: one row per stream the session may read. `Lag_ms` is
/// its sequencer's lag, `NULL` while no
/// sequencer runs it.
async fn show(db: Option<&str>, cx: &Ctx<'_>, host: &mut dyn Host) -> Result<Handled> {
    let db_name = db.or(cx.current).ok_or_else(no_db)?;
    let id = host.db_id(db_name).ok_or_else(|| unknown_db(db_name))?;
    let shown: Vec<&std::sync::Arc<StreamDef>> = cx
        .state
        .of_db(id)
        .filter(|s| s.state == StreamState::Active)
        .filter(|s| host.allowed(Privilege::Select, db_name, None, &s.name))
        .collect();
    let lags = if shown.is_empty() {
        std::collections::HashMap::new()
    } else {
        host.lags().await
    };
    let rows = shown
        .into_iter()
        .map(|s| {
            vec![
                Some(s.name.clone()),
                Some(s.kind.as_str().to_owned()),
                Some(s.partitions.to_string()),
                Some(s.cleanup.as_str().to_owned()),
                s.source_name.clone(),
                Some(s.key_spec.join(",")),
                lags.get(&s.id).map(u64::to_string),
            ]
        })
        .collect();
    Ok(Handled::Rows {
        columns: [
            "Stream",
            "Kind",
            "Partitions",
            "Cleanup",
            "Source",
            "Key",
            "Lag_ms",
        ]
        .iter()
        .map(|c| (*c).to_owned())
        .collect(),
        rows,
    })
}

/// The column of the outbox a stream column writes.
fn outbox_column(c: &str) -> Option<&'static str> {
    match c.to_ascii_lowercase().as_str() {
        "key" | "k" => Some("k"),
        "value" | "v" => Some("v"),
        "headers" => Some("headers"),
        "partition_no" => Some("partition_no"),
        "ts" => Some("ts"),
        _ => None,
    }
}

/// `INSERT INTO <stream>` as an insert into its outbox (logical names).
///
/// # Errors
/// 39941, 39944, 1054, 1142, 1235.
fn publish(
    sql: &str,
    i: &InsertInto,
    db_name: &str,
    s: &StreamDef,
    cx: &Ctx<'_>,
    host: &mut dyn Host,
) -> Result<Handled> {
    crate::publish::check_target(s)?;
    if i.on_duplicate {
        return Err(append_only(
            "INSERT ... ON DUPLICATE KEY UPDATE on a stream",
        ));
    }
    if i.set_form {
        return Err(SprigError::standard(
            1235,
            *b"42000",
            "This version of SprigDB doesn't yet support 'INSERT ... SET' on a stream: use VALUES or SELECT",
        ));
    }
    if !host.allowed(Privilege::Insert, db_name, None, &s.name) {
        return Err(denied(host, Privilege::Insert, &s.name));
    }
    let max = host.settings(s.db).max_message_bytes;
    if i.longest_literal as u64 > max {
        return Err(SprigError::new(
            SprigCode::StreamRecordTooLarge,
            format!(
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a value of {} bytes is above stream.max_message_bytes ({max})",
                i.longest_literal
            ),
        ));
    }
    let target = match &i.target.db {
        Some(d) => format!(
            "{}.{}",
            sprig_common::names::quote(d),
            sprig_common::names::quote(&s.outbox_table())
        ),
        None => sprig_common::names::quote(&s.outbox_table()),
    };
    let mut out = String::with_capacity(sql.len() + 32);
    out.push_str(&sql[..i.target.range.start]);
    out.push_str(&target);
    if let Some(cols) = &i.columns {
        {
            let mut last = i.target.range.end;
            for (name, r) in cols {
                let col = outbox_column(name).ok_or_else(|| {
                    SprigError::standard(
                        1054,
                        *b"42S22",
                        format!("Unknown column '{name}' in 'field list'"),
                    )
                })?;
                out.push_str(&sql[last..r.start]);
                out.push_str(col);
                last = r.end;
            }
            out.push_str(&sql[last..]);
        }
    } else {
        {
            let cols = if i.first_row_arity == Some(2) {
                " (k, v)"
            } else {
                " (k, v, headers)"
            };
            out.push_str(cols);
            out.push_str(&sql[i.after_target..]);
        }
    }
    let _ = cx;
    Ok(Handled::Forward {
        sql: out,
        write: true,
        compact: s.cleanup == Cleanup::Compact,
    })
}

async fn select(sql: &str, cx: &Ctx<'_>, host: &mut dyn Host) -> Result<Handled> {
    if cx.state.is_empty() {
        return Ok(Handled::NotStream);
    }
    let ids = grammar::identifiers(sql.as_bytes(), &cx.lex);
    if !ids.iter().any(|w| cx.state.mentions(w, cx.lctn)) {
        return Ok(Handled::NotStream);
    }
    let mut found: BTreeMap<(String, String), std::sync::Arc<StreamDef>> = BTreeMap::new();
    let r = {
        let h: &dyn Host = &*host;
        let lookup = |db: Option<&str>, name: &str| -> Option<StreamId> {
            let q = QName {
                db: db.map(str::to_owned),
                name: name.to_owned(),
                range: 0..0,
            };
            stream_of(cx, h, &q).map(|(_, s)| s.id)
        };
        let Some(r) = read::rewrite_select(sql, &cx.lex, &lookup)? else {
            return Ok(Handled::NotStream);
        };
        for (db, name, _) in &r.streams {
            let q = QName {
                db: db.clone(),
                name: name.clone(),
                range: 0..0,
            };
            if let Some((d, s)) = stream_of(cx, h, &q) {
                found.insert((d, s.name.clone()), s);
            }
        }
        r
    };
    // A view, routine, trigger or event over a
    // change topic would read the log with its definer's rights, so its
    // readers would get the table's rows without the table's policies: it
    // reads the topic's filter view instead, which applies each reader's
    // own policies (the host deploys it; a host that can't refuses).
    let defines = ids
        .first()
        .is_some_and(|w| w.eq_ignore_ascii_case("CREATE") || w.eq_ignore_ascii_case("ALTER"));
    // Privileges, and the row-level filter for change topics (a
    // session the table's policies bind reads the records whose images
    // they show).
    let mut filters = std::collections::HashMap::new();
    for ((db, _), s) in &found {
        match (s.kind, s.source_object) {
            (StreamKind::Changes, Some(o)) => {
                let src = s.source_name.clone().unwrap_or_default();
                if !host.allowed(Privilege::Select, db, Some(o), &src) {
                    return Err(denied(&*host, Privilege::Select, &s.name));
                }
                if !defines
                    && host.row_filtered(db, o)
                    && let Some(f) = host.change_filter(db, s).await?
                {
                    filters.insert(s.id, f);
                }
            }
            _ => {
                if !host.allowed(Privilege::Select, db, None, &s.name) {
                    return Err(denied(&*host, Privilege::Select, &s.name));
                }
            }
        }
    }
    if defines {
        let mut sql = r.sql;
        for s in found.values().filter(|s| s.kind == StreamKind::Changes) {
            host.change_view(s).await?;
            sql = sql.replace(
                &sprig_common::names::quote(&crate::model::log_table(s.id)),
                &sprig_common::names::quote(&format!("__sprig_scf_{}", s.id.0)),
            );
        }
        return Ok(Handled::Forward {
            sql,
            write: false,
            compact: false,
        });
    }
    if filters.is_empty() {
        return Ok(Handled::Forward {
            sql: r.sql,
            write: false,
            compact: false,
        });
    }
    let h: &dyn Host = &*host;
    let lookup = |db: Option<&str>, name: &str| -> Option<StreamId> {
        let q = QName {
            db: db.map(str::to_owned),
            name: name.to_owned(),
            range: 0..0,
        };
        stream_of(cx, h, &q).map(|(_, s)| s.id)
    };
    let r = read::rewrite_select_filtered(sql, &cx.lex, &lookup, &filters)?
        .ok_or_else(|| SprigError::internal("a stream read that no longer parses"))?;
    Ok(Handled::Forward {
        sql: r.sql,
        write: false,
        compact: false,
    })
}

/// Makes table `table` of `db`'s change topic match its effective
/// `stream.change_topic`: creates `<table>.changes` when on and missing,
/// drops it when off. Returns whether anything changed.
///
/// # Errors
/// The host's.
pub async fn reconcile_change_topic(
    table: &QName,
    cx: &Ctx<'_>,
    host: &mut dyn Host,
) -> Result<bool> {
    let db_name = db_of(table, cx)?.to_owned();
    let db = host.db_id(&db_name).ok_or_else(|| unknown_db(&db_name))?;
    let Some(info) = host.table(db, &table.name) else {
        return Ok(false);
    };
    let on = host.change_topic_on(db, info.object);
    let existing = cx.state.change_topic_of(db, info.object).cloned();
    match (on, existing) {
        (true, None) => {
            let name = change_topic_name(&info.name);
            if !valid_name(&name) || cx.state.get(db, &name, cx.lctn).is_some() {
                return Err(SprigError::new(
                    SprigCode::StreamExists,
                    format!("ER_SPRIG_STREAM_EXISTS: '{db_name}.{name}' exists"),
                ));
            }
            let template = host
                .template(db)
                .ok_or_else(|| SprigError::internal(format!("database {db_name} has no shard")))?;
            let partitions = host.settings(db).partitions_default.max(1);
            let id = StreamId(host.next_id().await?);
            let pos = host.position(&template).await?;
            let start = Start::At {
                cluster: template.cluster.clone(),
                pos: pos.parse()?,
            };
            let def = StreamDef {
                id,
                db,
                name,
                kind: StreamKind::Changes,
                source_object: Some(info.object),
                source_name: Some(info.name.clone()),
                partitions,
                key_spec: Vec::new(),
                attached_group: None,
                log_group: template.slot.group,
                cleanup: Cleanup::Delete,
                envelope: Envelope::ChangeJson,
                state: StreamState::Active,
                outbox: Vec::new(),
                log: vec![LogShard {
                    slot: template.slot,
                    partitions: Vec::new(),
                }],
                created_at_ms: u64::try_from(crate::maria::wall_ms()).unwrap_or(0),
            };
            host.ddl(
                &template,
                &log_statements(&template, id, partitions, &start),
            )
            .await?;
            if let Err(e) = host.commit(&StreamsChange::Insert(def.clone(), None)).await {
                let _ = host
                    .ddl(&template, &ddl::drop_log_sql(&template.schema, id))
                    .await;
                return Err(e);
            }
            host.subscribe(&def).await?;
            tracing::info!(tenant = %host.tenant(), stream = id.0, table = %info.name, "change topic created");
            Ok(true)
        }
        (false, Some(s)) => {
            remove(&s, host).await?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// `ALTER TABLE t SPRIG SET | RESET (... stream.change_topic ...)`: the
/// table, so the gateway can reconcile after the settings statement.
#[must_use]
pub fn change_topic_target(sql: &str, lex: &LexOptions) -> Option<QName> {
    if !sql.to_ascii_lowercase().contains("stream.change_topic") {
        return None;
    }
    let toks = sprig_sql::lex::lex(sql.as_bytes(), lex).ok()?;
    let words: Vec<_> = toks.iter().filter(|t| !t.kind.is_trivia()).collect();
    let w = |i: usize, s: &str| {
        words
            .get(i)
            .is_some_and(|t| sql[t.range()].eq_ignore_ascii_case(s))
    };
    if !(w(0, "ALTER") && w(1, "TABLE")) {
        return None;
    }
    let ident = |i: usize| -> Option<(String, std::ops::Range<usize>)> {
        let t = words.get(i)?;
        if !t.kind.is_ident_like() {
            return None;
        }
        Some((
            String::from_utf8_lossy(&sprig_sql::lex::ident_value(t.bytes(sql.as_bytes())))
                .into_owned(),
            t.range(),
        ))
    };
    let (first, r) = ident(2)?;
    if words.get(3).is_some_and(|t| &sql[t.range()] == ".") {
        let (second, r2) = ident(4)?;
        if !w(5, "SPRIG") {
            return None;
        }
        return Some(QName {
            db: Some(first),
            name: second,
            range: r.start..r2.end,
        });
    }
    if !w(3, "SPRIG") {
        return None;
    }
    Some(QName {
        db: None,
        name: first,
        range: r,
    })
}

/// The group id of a slot list's first slot (for messages).
#[must_use]
pub fn group_of(slots: &[PhysSlot]) -> Option<GroupId> {
    slots.first().map(|s| s.slot.group)
}

#[cfg(test)]
mod tests;
