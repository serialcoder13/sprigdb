//! The `mq` catalog module (chapter 50, "Catalog tables"; build step 2):
//! `mq_vhosts`, `mq_exchanges`, `mq_queues`, `mq_bindings`, `mq_policies`,
//! `mq_permissions` and `mq_keys` in `t<tenant>_sprig`, their migration,
//! the snapshot loader ([`MqExtension`]) and the writes declares make.
//!
//! Every declare, delete, bind and unbind of a durable object is one
//! versioned catalog change, so every gateway's routing follows it.
//! Temporary queues are not here (the lead's choice): see
//! [`crate::topology::TempSet`].
//!
//! **As built, against the book:**
//! - Exchanges and queues have no `objects` row (the `objects.kind` enum
//!   has no `exchange` or `queue` yet, as streams have none): their ids
//!   come from the object counter.
//! - `arguments` are JSON with RabbitMQ's value types folded (integers to
//!   numbers, strings, booleans, nested tables and arrays; byte strings as
//!   `{"$bytes": base64}`).
//! - `mq_keys` holds the tenant's delivery-token key (one row, made on
//!   first use).

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use mysql_async::Row;
use mysql_async::prelude::Queryable;
use serde_json::{Map, Value, json};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_amqp::backend::DestKind;
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};

use crate::engine::TopoChange;
use crate::topology::{
    ApplyTo, BindingDef, ExKind, ExchangeDef, PermissionDef, PolicyDef, QType, QueueDef, Topology,
};

/// The module name.
pub const MODULE: &str = "mq";

const DDL_VHOSTS: &str = "CREATE TABLE IF NOT EXISTS mq_vhosts (
  vhost_name         VARCHAR(255) NOT NULL,
  db_id              INT UNSIGNED NOT NULL,
  default_queue_type ENUM('classic','quorum','stream') NOT NULL DEFAULT 'classic',
  limits             JSON NULL,
  description        VARCHAR(1024) NULL,
  PRIMARY KEY (vhost_name), UNIQUE KEY uk_db (db_id)
) ENGINE=InnoDB";

const DDL_EXCHANGES: &str = "CREATE TABLE IF NOT EXISTS mq_exchanges (
  object_id        BIGINT UNSIGNED NOT NULL,
  db_id            INT UNSIGNED NOT NULL,
  name             VARBINARY(255) NOT NULL,
  type             ENUM('direct','fanout','topic','headers','x-delayed-message') NOT NULL,
  delayed_type     ENUM('direct','fanout','topic','headers') NULL,
  durable          BOOLEAN NOT NULL,
  auto_delete      BOOLEAN NOT NULL,
  internal         BOOLEAN NOT NULL,
  arguments        JSON NULL,
  change_source_id BIGINT UNSIGNED NULL,
  PRIMARY KEY (object_id), UNIQUE KEY uk_name (db_id, name)
) ENGINE=InnoDB";

const DDL_QUEUES: &str = "CREATE TABLE IF NOT EXISTS mq_queues (
  object_id        BIGINT UNSIGNED NOT NULL,
  db_id            INT UNSIGNED NOT NULL,
  name             VARBINARY(255) NOT NULL,
  qtype            ENUM('classic','quorum','stream') NOT NULL,
  durable          BOOLEAN NOT NULL,
  auto_delete      BOOLEAN NOT NULL,
  exclusive_owner  VARBINARY(64) NULL,
  arguments        JSON NULL,
  lanes            SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  stream_id        BIGINT UNSIGNED NULL,
  state            ENUM('active','deleting') NOT NULL,
  last_used_at     DATETIME(3) NOT NULL,
  PRIMARY KEY (object_id), UNIQUE KEY uk_name (db_id, name)
) ENGINE=InnoDB";

const DDL_BINDINGS: &str = "CREATE TABLE IF NOT EXISTS mq_bindings (
  db_id            INT UNSIGNED NOT NULL,
  source_id        BIGINT UNSIGNED NOT NULL,
  dest_id          BIGINT UNSIGNED NOT NULL,
  dest_kind        ENUM('queue','exchange') NOT NULL,
  binding_key      VARBINARY(255) NOT NULL,
  arguments_hash   BINARY(8) NOT NULL,
  arguments        JSON NULL,
  PRIMARY KEY (db_id, source_id, dest_id, binding_key, arguments_hash),
  KEY k_dest (dest_id)
) ENGINE=InnoDB";

const DDL_POLICIES: &str = "CREATE TABLE IF NOT EXISTS mq_policies (
  db_id            INT UNSIGNED NOT NULL,
  name             VARCHAR(255) NOT NULL,
  kind             ENUM('user','operator') NOT NULL,
  pattern          VARCHAR(1024) NOT NULL,
  apply_to         ENUM('queues','classic_queues','quorum_queues','streams','exchanges','all') NOT NULL,
  priority         INT NOT NULL,
  definition       JSON NOT NULL,
  PRIMARY KEY (db_id, kind, name)
) ENGINE=InnoDB";

const DDL_PERMISSIONS: &str = "CREATE TABLE IF NOT EXISTS mq_permissions (
  db_id            INT UNSIGNED NOT NULL,
  user_id          INT UNSIGNED NOT NULL,
  configure_re     VARCHAR(1024) NOT NULL,
  write_re         VARCHAR(1024) NOT NULL,
  read_re          VARCHAR(1024) NOT NULL,
  PRIMARY KEY (db_id, user_id)
) ENGINE=InnoDB";

const DDL_KEYS: &str = "CREATE TABLE IF NOT EXISTS mq_keys (
  kid              TINYINT UNSIGNED NOT NULL,
  secret           VARBINARY(64) NOT NULL,
  PRIMARY KEY (kid)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MQ_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "mq_vhosts, mq_exchanges, mq_queues, mq_bindings, mq_policies, mq_permissions, mq_keys",
    statements: &[
        DDL_VHOSTS,
        DDL_EXCHANGES,
        DDL_QUEUES,
        DDL_BINDINGS,
        DDL_POLICIES,
        DDL_PERMISSIONS,
        DDL_KEYS,
    ],
}];

/// The `mq` module, for [`Migrator::new`].
pub const MQ_MODULE: Module = Module {
    name: MODULE,
    migrations: MQ_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema` (idempotent).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[MQ_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("mq catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("mq catalog: {what}"))
}

// ----------------------------------------------------------------------
// Arguments as JSON.
// ----------------------------------------------------------------------

/// A field value as JSON.
#[must_use]
pub fn value_json(v: &FieldValue) -> Value {
    match v {
        FieldValue::Bool(b) => json!(b),
        FieldValue::I8(_)
        | FieldValue::U8(_)
        | FieldValue::I16(_)
        | FieldValue::U16(_)
        | FieldValue::I32(_)
        | FieldValue::U32(_)
        | FieldValue::I64(_)
        | FieldValue::Timestamp(_) => v.as_i64().map_or(Value::Null, |n| json!(n)),
        FieldValue::F32(f) => json!(f),
        FieldValue::F64(f) => json!(f),
        FieldValue::Decimal(s, u) => json!({"$decimal": [s, u]}),
        FieldValue::LongStr(b) => match std::str::from_utf8(b) {
            Ok(s) => json!(s),
            Err(_) => json!({"$bytes": STANDARD.encode(b)}),
        },
        FieldValue::Bytes(b) => json!({"$bytes": STANDARD.encode(b)}),
        FieldValue::Array(a) => Value::Array(a.iter().map(value_json).collect()),
        FieldValue::Table(t) => table_json(t),
        FieldValue::Void => Value::Null,
    }
}

/// A field table as a JSON object.
#[must_use]
pub fn table_json(t: &FieldTable) -> Value {
    Value::Object(
        t.0.iter()
            .map(|(k, v)| (k.clone(), value_json(v)))
            .collect::<Map<_, _>>(),
    )
}

/// JSON back to a field value.
#[must_use]
pub fn json_value(v: &Value) -> FieldValue {
    match v {
        Value::Null => FieldValue::Void,
        Value::Bool(b) => FieldValue::Bool(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => FieldValue::I64(i),
            None => FieldValue::F64(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => FieldValue::str(s),
        Value::Array(a) => FieldValue::Array(a.iter().map(json_value).collect()),
        Value::Object(o) => {
            if let Some(Value::String(b)) = o.get("$bytes")
                && o.len() == 1
            {
                return FieldValue::Bytes(STANDARD.decode(b).unwrap_or_default().into());
            }
            FieldValue::Table(json_table(v))
        }
    }
}

/// A JSON object as a field table.
#[must_use]
pub fn json_table(v: &Value) -> FieldTable {
    match v {
        Value::Object(o) => FieldTable(o.iter().map(|(k, v)| (k.clone(), json_value(v))).collect()),
        _ => FieldTable::new(),
    }
}

fn args_hash(t: &FieldTable) -> [u8; 8] {
    let text = table_json(t).to_string();
    let d = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&d.as_ref()[..8]);
    out
}

// ----------------------------------------------------------------------
// The snapshot state.
// ----------------------------------------------------------------------

/// The `mq` module in a snapshot: each database's durable model.
#[derive(Debug, Clone, Default)]
pub struct MqState {
    /// The module's tables exist.
    pub migrated: bool,
    /// By database id.
    pub dbs: HashMap<u32, Arc<Topology>>,
}

impl ExtState for MqState {
    fn bytes(&self) -> usize {
        self.dbs
            .values()
            .map(|t| 256 * (t.exchanges.len() + t.queues.len() + t.bindings.len()))
            .sum()
    }
}

impl MqState {
    /// A database's model (the built-ins only when it has none).
    #[must_use]
    pub fn topology(&self, db: u32, version: u64) -> Arc<Topology> {
        self.dbs.get(&db).cloned().unwrap_or_else(|| {
            let mut t = Topology::new();
            t.version = version;
            Arc::new(t)
        })
    }
}

fn col<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| bad(&format!("bad column {i}")))
}

fn text(b: Vec<u8>) -> String {
    String::from_utf8(b).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn parse_json(s: Option<String>) -> Value {
    s.and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(Value::Null)
}

/// Reads and writes the module's tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct MqStore {
    schema: String,
}

impl MqStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", quote(&self.schema))
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'mq_keys'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Every database's model.
    ///
    /// # Errors
    /// A backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q, version: u64) -> Result<MqState> {
        let mut topo: HashMap<u32, Topology> = HashMap::new();
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT object_id, db_id, name, type, durable, auto_delete, internal, \
                 CAST(arguments AS CHAR) FROM {}",
                self.t("mq_exchanges")
            ))
            .await
            .map_err(map_err)?;
        let mut names: HashMap<u64, (String, DestKind)> = HashMap::new();
        for r in &rows {
            let id: u64 = col(r, 0)?;
            let db: u32 = col(r, 1)?;
            let name = text(col(r, 2)?);
            let kind: String = col(r, 3)?;
            let x = ExchangeDef {
                id,
                name: name.clone(),
                kind: ExKind::parse(&kind).ok_or_else(|| bad("exchange type"))?,
                durable: col(r, 4)?,
                auto_delete: col(r, 5)?,
                internal: col(r, 6)?,
                arguments: json_table(&parse_json(col(r, 7)?)),
                builtin: false,
            };
            names.insert(id, (name.clone(), DestKind::Exchange));
            let t = topo.entry(db).or_insert_with(|| {
                let mut t = Topology::new();
                t.version = version;
                t
            });
            t.exchanges.insert(name, Arc::new(x));
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT object_id, db_id, name, qtype, durable, auto_delete, \
                 CAST(arguments AS CHAR), lanes FROM {} WHERE state = 'active'",
                self.t("mq_queues")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let id: u64 = col(r, 0)?;
            let db: u32 = col(r, 1)?;
            let name = text(col(r, 2)?);
            let qtype: String = col(r, 3)?;
            let qd = QueueDef {
                id,
                name: name.clone(),
                qtype: QType::parse(&qtype).ok_or_else(|| bad("qtype"))?,
                durable: col(r, 4)?,
                exclusive: None,
                auto_delete: col(r, 5)?,
                arguments: json_table(&parse_json(col(r, 6)?)),
                lanes: col(r, 7)?,
                temp_on: None,
            };
            names.insert(id, (name.clone(), DestKind::Queue));
            let t = topo.entry(db).or_insert_with(|| {
                let mut t = Topology::new();
                t.version = version;
                t
            });
            t.queues.insert(name, Arc::new(qd));
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT db_id, source_id, dest_id, dest_kind, binding_key, CAST(arguments AS CHAR) \
                 FROM {}",
                self.t("mq_bindings")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let db: u32 = col(r, 0)?;
            let (Some((source, _)), Some((dest, kind))) =
                (names.get(&col::<u64>(r, 1)?), names.get(&col::<u64>(r, 2)?))
            else {
                continue;
            };
            let b = BindingDef {
                source: source.clone(),
                dest: dest.clone(),
                dest_kind: *kind,
                key: text(col(r, 4)?),
                arguments: json_table(&parse_json(col(r, 5)?)),
            };
            if let Some(t) = topo.get_mut(&db) {
                t.bindings.push(b);
            }
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT db_id, name, kind, pattern, apply_to, priority, CAST(definition AS CHAR) \
                 FROM {} ORDER BY priority DESC, name",
                self.t("mq_policies")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let db: u32 = col(r, 0)?;
            let pattern: String = col(r, 3)?;
            let Ok(re) = regex::Regex::new(&pattern) else {
                continue;
            };
            let apply: String = col(r, 4)?;
            let kind: String = col(r, 2)?;
            let p = PolicyDef {
                name: col(r, 1)?,
                operator: kind == "operator",
                pattern: re,
                apply_to: ApplyTo::parse(&apply).unwrap_or(ApplyTo::All),
                priority: col(r, 5)?,
                definition: parse_json(col(r, 6)?)
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
            };
            topo.entry(db)
                .or_insert_with(|| {
                    let mut t = Topology::new();
                    t.version = version;
                    t
                })
                .policies
                .push(p);
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT db_id, user_id, configure_re, write_re, read_re FROM {}",
                self.t("mq_permissions")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let db: u32 = col(r, 0)?;
            let re = |i: usize| -> Result<regex::Regex> {
                let s: String = col(r, i)?;
                regex::Regex::new(&s).map_err(|_| bad("permission regex"))
            };
            let (Ok(c), Ok(w), Ok(rd)) = (re(2), re(3), re(4)) else {
                continue;
            };
            topo.entry(db)
                .or_insert_with(|| {
                    let mut t = Topology::new();
                    t.version = version;
                    t
                })
                .permissions
                .push(PermissionDef {
                    user_id: col(r, 1)?,
                    configure: c,
                    write: w,
                    read: rd,
                });
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT db_id, CAST(limits AS CHAR) FROM {}",
                self.t("mq_vhosts")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let db: u32 = col(r, 0)?;
            let limits = parse_json(col(r, 1)?);
            if let Some(n) = limits.get("max-queues").and_then(Value::as_u64) {
                topo.entry(db)
                    .or_insert_with(|| {
                        let mut t = Topology::new();
                        t.version = version;
                        t
                    })
                    .max_queues = Some(n);
            }
        }
        Ok(MqState {
            migrated: true,
            dbs: topo.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
        })
    }

    async fn id_of<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: u32,
        name: &str,
        kind: DestKind,
    ) -> Result<Option<u64>> {
        let table = match kind {
            DestKind::Exchange => "mq_exchanges",
            DestKind::Queue => "mq_queues",
        };
        q.exec_first(
            format!(
                "SELECT object_id FROM {} WHERE db_id = ? AND name = ? FOR UPDATE",
                self.t(table)
            ),
            (db, name.as_bytes()),
        )
        .await
        .map_err(map_err)
    }

    /// Applies a change inside the caller's catalog transaction. `new_id`
    /// is used for a new exchange or queue. Declaring a name that exists
    /// (raced by another gateway) is a no-op.
    ///
    /// # Errors
    /// A backend error.
    pub async fn apply<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: u32,
        change: &TopoChange,
        new_id: u64,
    ) -> Result<()> {
        match change {
            TopoChange::AddExchange(x) => {
                if self.id_of(q, db, &x.name, DestKind::Exchange).await?.is_some() {
                    return Ok(());
                }
                let delayed = (x.kind == ExKind::Delayed).then(|| x.route_kind().as_str());
                q.exec_drop(
                    format!(
                        "INSERT INTO {} (object_id, db_id, name, type, delayed_type, durable, \
                         auto_delete, internal, arguments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        self.t("mq_exchanges")
                    ),
                    mysql_async::Params::Positional(vec![
                        new_id.into(),
                        db.into(),
                        x.name.as_bytes().into(),
                        x.kind.as_str().into(),
                        delayed.into(),
                        x.durable.into(),
                        x.auto_delete.into(),
                        x.internal.into(),
                        table_json(&x.arguments).to_string().into(),
                    ]),
                )
                .await
                .map_err(map_err)
            }
            TopoChange::DelExchange(name) => {
                let Some(id) = self.id_of(q, db, name, DestKind::Exchange).await? else {
                    return Ok(());
                };
                q.exec_drop(
                    format!(
                        "DELETE FROM {} WHERE db_id = ? AND (source_id = ? OR (dest_id = ? AND dest_kind = 'exchange'))",
                        self.t("mq_bindings")
                    ),
                    (db, id, id),
                )
                .await
                .map_err(map_err)?;
                q.exec_drop(
                    format!("DELETE FROM {} WHERE object_id = ?", self.t("mq_exchanges")),
                    (id,),
                )
                .await
                .map_err(map_err)
            }
            TopoChange::AddQueue(qd) => {
                if self.id_of(q, db, &qd.name, DestKind::Queue).await?.is_some() {
                    return Ok(());
                }
                q.exec_drop(
                    format!(
                        "INSERT INTO {} (object_id, db_id, name, qtype, durable, auto_delete, \
                         arguments, lanes, state, last_used_at) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', UTC_TIMESTAMP(3))",
                        self.t("mq_queues")
                    ),
                    mysql_async::Params::Positional(vec![
                        new_id.into(),
                        db.into(),
                        qd.name.as_bytes().into(),
                        qd.qtype.as_str().into(),
                        qd.durable.into(),
                        qd.auto_delete.into(),
                        table_json(&qd.arguments).to_string().into(),
                        qd.lanes.into(),
                    ]),
                )
                .await
                .map_err(map_err)
            }
            TopoChange::DelQueue(name) => {
                let Some(id) = self.id_of(q, db, name, DestKind::Queue).await? else {
                    return Ok(());
                };
                q.exec_drop(
                    format!(
                        "DELETE FROM {} WHERE db_id = ? AND dest_id = ? AND dest_kind = 'queue'",
                        self.t("mq_bindings")
                    ),
                    (db, id),
                )
                .await
                .map_err(map_err)?;
                q.exec_drop(
                    format!("DELETE FROM {} WHERE object_id = ?", self.t("mq_queues")),
                    (id,),
                )
                .await
                .map_err(map_err)
            }
            TopoChange::Bind(b) | TopoChange::Unbind(b) => {
                let (Some(src), Some(dst)) = (
                    self.id_of(q, db, &b.source, DestKind::Exchange).await?,
                    self.id_of(q, db, &b.dest, b.dest_kind).await?,
                ) else {
                    return Ok(());
                };
                let kind = match b.dest_kind {
                    DestKind::Queue => "queue",
                    DestKind::Exchange => "exchange",
                };
                let hash = args_hash(&b.arguments);
                if matches!(change, TopoChange::Bind(_)) {
                    q.exec_drop(
                        format!(
                            "INSERT IGNORE INTO {} (db_id, source_id, dest_id, dest_kind, binding_key, \
                             arguments_hash, arguments) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            self.t("mq_bindings")
                        ),
                        (
                            db,
                            src,
                            dst,
                            kind,
                            b.key.as_bytes(),
                            hash.to_vec(),
                            table_json(&b.arguments).to_string(),
                        ),
                    )
                    .await
                    .map_err(map_err)
                } else {
                    q.exec_drop(
                        format!(
                            "DELETE FROM {} WHERE db_id = ? AND source_id = ? AND dest_id = ? \
                             AND binding_key = ? AND arguments_hash = ?",
                            self.t("mq_bindings")
                        ),
                        (db, src, dst, b.key.as_bytes(), hash.to_vec()),
                    )
                    .await
                    .map_err(map_err)
                }
            }
        }
    }

    /// The tenant's token key, made on first use.
    ///
    /// # Errors
    /// A backend error.
    pub async fn token_secret<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<u8>> {
        let mut secret = [0u8; 32];
        let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut secret);
        q.exec_drop(
            format!(
                "INSERT IGNORE INTO {} (kid, secret) VALUES (1, ?)",
                self.t("mq_keys")
            ),
            (secret.to_vec(),),
        )
        .await
        .map_err(map_err)?;
        let s: Option<Vec<u8>> = q
            .exec_first(
                format!("SELECT secret FROM {} WHERE kid = 1", self.t("mq_keys")),
                (),
            )
            .await
            .map_err(map_err)?;
        s.ok_or_else(|| bad("no token key"))
    }
}

/// The module in the catalog cache: [`MqState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct MqExtension;

#[async_trait::async_trait]
impl CatalogExtension for MqExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        MQ_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = MqStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(MqState::default()));
        }
        Ok(Arc::new(store.load(conn, 0).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[MQ_MODULE]).unwrap_or_else(|_| unreachable!());
        assert_eq!(MQ_MODULE.newest(), 1);
    }

    #[test]
    fn arguments_round_trip() {
        let t = FieldTable::from([
            ("x-match", FieldValue::str("any")),
            ("n", FieldValue::I32(7)),
            ("b", FieldValue::Bool(true)),
            ("raw", FieldValue::Bytes(bytes::Bytes::from_static(&[0, 1, 255]))),
            ("nested", FieldValue::Table(FieldTable::from([("k", FieldValue::I64(1))]))),
        ]);
        let back = json_table(&table_json(&t));
        assert_eq!(back.get("x-match"), Some(&FieldValue::str("any")));
        assert_eq!(back.get("n").and_then(FieldValue::as_i64), Some(7));
        assert_eq!(back.get("raw"), t.get("raw"));
        assert_eq!(args_hash(&t), args_hash(&t.clone()));
    }
}
