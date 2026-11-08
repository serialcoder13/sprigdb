//! The physical side of the DDL path: statements on the tenant's cluster
//! ([`DdlExecutor`]) and what the reconciler reads there.
//!
//! An executor is bound to one tenant's cluster (R0 keeps every database
//! of a tenant on the catalog's cluster). The gateway passes one that runs
//! on the session's backend account; the controller and tests use
//! [`MariaExecutor`] over a pool, and unit tests [`MemExecutor`](super::mem::MemExecutor).
//!
//! Errors tell the DDL path what happened:
//! - a server error ([`ErrorKind::Backend`](sprig_common::ErrorKind)) is
//!   definite: MariaDB 10.6+ DDL is atomic, so nothing changed;
//! - 39030 `ER_SPRIG_BACKEND_UNAVAILABLE` means the statement was never
//!   sent: nothing changed;
//! - anything else (39032 `ER_SPRIG_OUTCOME_UNKNOWN`, timeouts) leaves the
//!   outcome unknown, and the reconciler decides from the physical state.

use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Pool, Row};
use sprig_common::ids::TenantId;
use sprig_common::names::{SchemaName, quote};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use crate::store::ObjectKind;

/// What a physical statement does, so in-memory executors can model it and
/// logs can say it without parsing SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// `CREATE DATABASE` of the statement's schema.
    CreateSchema,
    /// `ALTER DATABASE` of the statement's schema.
    AlterSchema,
    /// `DROP DATABASE` of the statement's schema.
    DropSchema,
    /// Creates (or replaces) an object in the schema.
    Create {
        /// Its kind.
        kind: ObjectKind,
        /// Its name.
        name: String,
    },
    /// Changes an object's definition (`ALTER`, `CREATE INDEX`, ...).
    Alter {
        /// Its kind.
        kind: ObjectKind,
        /// Its name.
        name: String,
    },
    /// Drops objects of one kind from the schema.
    Drop {
        /// Their kind.
        kind: ObjectKind,
        /// Their names.
        names: Vec<String>,
    },
    /// Renames tables, possibly across schemas: `(from schema, from name,
    /// to schema, to name)` in order.
    Rename {
        /// The moves.
        moves: Vec<(SchemaName, String, SchemaName, String)>,
    },
    /// Empties a table.
    Truncate {
        /// The table.
        name: String,
    },
}

impl Effect {
    /// Whether the statement acts on the schema itself (runs without a
    /// default database).
    #[must_use]
    pub fn on_schema(&self) -> bool {
        matches!(
            self,
            Effect::CreateSchema | Effect::AlterSchema | Effect::DropSchema
        )
    }
}

/// One statement for the tenant's cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalDdl {
    /// The schema the statement acts on, or runs in (its default
    /// database).
    pub schema: SchemaName,
    /// The SQL, with physical schema names.
    pub sql: String,
    /// What it does.
    pub effect: Effect,
}

/// A schema's defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    /// Default character set.
    pub charset: String,
    /// Default collation.
    pub collation: String,
}

/// An object found in a physical schema.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalObject {
    /// Its kind.
    pub kind: ObjectKind,
    /// Its name.
    pub name: String,
    /// For a trigger, its table.
    pub parent: Option<String>,
}

/// Runs DDL on, and reads the physical state of, one tenant's cluster.
#[async_trait::async_trait]
pub trait DdlExecutor: Send + Sync {
    /// Runs one statement: with `stmt.schema` as the default database,
    /// unless it acts on the schema itself.
    ///
    /// # Errors
    /// See the module docs.
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()>;

    /// The schema's defaults, or `None` if it doesn't exist.
    ///
    /// # Errors
    /// 39030 if the cluster is unreachable.
    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>>;

    /// `SHOW CREATE <kind>` text of an object, or `None` if it doesn't
    /// exist. Physical schema names in it are left as they are.
    ///
    /// # Errors
    /// 39030 if the cluster is unreachable.
    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>>;

    /// Every table, view, sequence, routine, trigger and event in the
    /// schema, internal (`__sprig_`) objects included, sorted.
    ///
    /// # Errors
    /// 39030 if the cluster is unreachable.
    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>>;

    /// Every data schema of `tenant` on the cluster (`t<tenant>_d...`).
    ///
    /// # Errors
    /// 39030 if the cluster is unreachable.
    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>>;

    /// Whether a DDL statement is running in the schema right now (the
    /// reconciler skips such schemas).
    ///
    /// # Errors
    /// 39030 if the cluster is unreachable.
    async fn busy(&self, _schema: &SchemaName) -> Result<bool> {
        Ok(false)
    }
}

/// Whether an executor error left the statement's outcome unknown.
#[must_use]
pub fn outcome_unknown(e: &SprigError) -> bool {
    !matches!(
        e.kind,
        ErrorKind::Backend { .. } | ErrorKind::Standard { .. }
    ) && e.code() != Some(SprigCode::BackendUnavailable)
}

/// The SQL keyword of a kind in `SHOW CREATE` and `DROP`.
#[must_use]
pub fn kind_keyword(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::View => "VIEW",
        ObjectKind::Sequence => "SEQUENCE",
        ObjectKind::Procedure => "PROCEDURE",
        ObjectKind::Function => "FUNCTION",
        ObjectKind::Trigger => "TRIGGER",
        ObjectKind::Event => "EVENT",
        _ => "TABLE",
    }
}

/// `DROP <kind> IF EXISTS` of objects in a schema: what the reconciler runs
/// to roll a drop forward.
#[must_use]
pub fn drop_sql(schema: &SchemaName, kind: ObjectKind, names: &[String]) -> String {
    let s = quote(&schema.to_string());
    let list: Vec<String> = names.iter().map(|n| format!("{s}.{}", quote(n))).collect();
    match kind {
        // One name per statement for these.
        ObjectKind::Procedure | ObjectKind::Function | ObjectKind::Trigger | ObjectKind::Event => {
            format!(
                "DROP {} IF EXISTS {}",
                kind_keyword(kind),
                list.first().map_or("", String::as_str)
            )
        }
        _ => format!("DROP {} IF EXISTS {}", kind_keyword(kind), list.join(", ")),
    }
}

/// [`DdlExecutor`] over a `mysql_async` pool to the tenant's cluster.
#[derive(Debug, Clone)]
pub struct MariaExecutor {
    pool: Pool,
    ddl_timeout: Duration,
    read_timeout: Duration,
}

impl MariaExecutor {
    /// Default deadline of one DDL statement.
    pub const DDL_TIMEOUT: Duration = Duration::from_secs(600);
    /// Default deadline of one metadata read.
    pub const READ_TIMEOUT: Duration = Duration::from_secs(10);

    /// Runs on `pool`.
    #[must_use]
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            ddl_timeout: Self::DDL_TIMEOUT,
            read_timeout: Self::READ_TIMEOUT,
        }
    }

    /// The same executor with other deadlines.
    #[must_use]
    pub fn with_timeouts(mut self, ddl: Duration, read: Duration) -> Self {
        self.ddl_timeout = ddl;
        self.read_timeout = read;
        self
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        match tokio::time::timeout(self.read_timeout, self.pool.get_conn()).await {
            Ok(Ok(c)) => Ok(c),
            Ok(Err(e)) => Err(unavailable("cluster unreachable").with_source(e)),
            Err(_) => Err(unavailable("cluster connection timed out")),
        }
    }

    async fn read<T, F>(&self, what: &'static str, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut mysql_async::Conn) -> std::result::Result<T, mysql_async::Error>,
    {
        let mut c = self.conn().await?;
        match tokio::time::timeout(self.read_timeout, f(&mut c)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(read_err(what, e)),
            Err(_) => Err(unavailable("metadata read timed out")),
        }
    }
}

fn unavailable(msg: &str) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, msg.to_owned())
}

fn server_err(e: &mysql_async::Error) -> Option<SprigError> {
    match e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            Some(SprigError::backend(s.code, state, s.message.clone()))
        }
        _ => None,
    }
}

fn read_err(what: &'static str, e: mysql_async::Error) -> SprigError {
    server_err(&e).unwrap_or_else(|| unavailable(what).with_source(e))
}

#[async_trait::async_trait]
impl DdlExecutor for MariaExecutor {
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        let mut c = self.conn().await?;
        if !stmt.effect.on_schema() {
            // Selecting the schema sends nothing that changes state.
            match tokio::time::timeout(
                self.read_timeout,
                c.query_drop(format!("USE {}", quote(&stmt.schema.to_string()))),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(read_err("USE", e)),
                Err(_) => return Err(unavailable("USE timed out")),
            }
        }
        match tokio::time::timeout(self.ddl_timeout, c.query_drop(stmt.sql.as_str())).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(server_err(&e).unwrap_or_else(|| {
                SprigError::new(
                    SprigCode::OutcomeUnknown,
                    "the backend was lost during DDL; the outcome is unknown",
                )
                .with_source(e)
            })),
            Err(_) => Err(SprigError::new(
                SprigCode::OutcomeUnknown,
                format!(
                    "DDL timed out after {} s; the outcome is unknown",
                    self.ddl_timeout.as_secs()
                ),
            )),
        }
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        let name = schema.to_string();
        let row: Option<(String, String)> = self
            .read("schema lookup", async |c| {
                c.exec_first(
                    "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME \
                     FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
                    (name,),
                )
                .await
            })
            .await?;
        Ok(row.map(|(charset, collation)| SchemaInfo { charset, collation }))
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        let sql = format!(
            "SHOW CREATE {} {}.{}",
            kind_keyword(kind),
            quote(&schema.to_string()),
            quote(name)
        );
        let r: std::result::Result<Option<Row>, SprigError> = self
            .read("SHOW CREATE", async |c| c.query_first::<Row, _>(sql).await)
            .await;
        let row = match r {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(None),
            // Missing table, view, routine, trigger, event, schema.
            Err(e)
                if matches!(
                    e.kind,
                    ErrorKind::Backend {
                        errno: 1146 | 1049 | 1305 | 1360 | 1539 | 4091,
                        ..
                    }
                ) =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let cols = row.columns_ref();
        let idx = cols.iter().position(|c| {
            let n = c.name_str();
            n.starts_with("Create ") || n == "SQL Original Statement"
        });
        let Some(idx) = idx else {
            return Err(SprigError::internal(format!(
                "SHOW CREATE {} returned no create column",
                kind_keyword(kind)
            )));
        };
        let v: Option<Option<String>> = row.get_opt(idx).and_then(std::result::Result::ok);
        Ok(v.flatten())
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        let name = schema.to_string();
        let n2 = name.clone();
        let n3 = name.clone();
        let n4 = name.clone();
        let tables: Vec<(String, String)> = self
            .read("tables", async |c| {
                c.exec(
                    "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = ?",
                    (name,),
                )
                .await
            })
            .await?;
        let routines: Vec<(String, String)> = self
            .read("routines", async |c| {
                c.exec(
                    "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES \
                     WHERE ROUTINE_SCHEMA = ?",
                    (n2,),
                )
                .await
            })
            .await?;
        let triggers: Vec<(String, String)> = self
            .read("triggers", async |c| {
                c.exec(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS \
                     WHERE EVENT_OBJECT_SCHEMA = ?",
                    (n3,),
                )
                .await
            })
            .await?;
        let events: Vec<String> = self
            .read("events", async |c| {
                c.exec(
                    "SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?",
                    (n4,),
                )
                .await
            })
            .await?;
        let mut out = Vec::new();
        for (n, t) in tables {
            let kind = match t.as_str() {
                "VIEW" | "SYSTEM VIEW" => ObjectKind::View,
                "SEQUENCE" => ObjectKind::Sequence,
                _ => ObjectKind::Table,
            };
            out.push(PhysicalObject {
                kind,
                name: n,
                parent: None,
            });
        }
        for (n, t) in routines {
            let kind = if t == "FUNCTION" {
                ObjectKind::Function
            } else {
                ObjectKind::Procedure
            };
            out.push(PhysicalObject {
                kind,
                name: n,
                parent: None,
            });
        }
        for (n, table) in triggers {
            out.push(PhysicalObject {
                kind: ObjectKind::Trigger,
                name: n,
                parent: Some(table),
            });
        }
        for n in events {
            out.push(PhysicalObject {
                kind: ObjectKind::Event,
                name: n,
                parent: None,
            });
        }
        out.sort();
        Ok(out)
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        // SHOW DATABASES lists directories; it opens no tables.
        let pattern = format!("t{tenant}\\_d%");
        let names: Vec<String> = self
            .read("schema list", async |c| {
                c.query(format!("SHOW DATABASES LIKE '{pattern}'")).await
            })
            .await?;
        Ok(names
            .iter()
            .filter_map(|n| n.parse::<SchemaName>().ok())
            .filter(|s| s.tenant() == tenant)
            .collect())
    }

    async fn busy(&self, schema: &SchemaName) -> Result<bool> {
        let name = schema.to_string();
        let n: Option<u64> = self
            .read("processlist", async |c| {
                c.exec_first(
                    "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = ? \
                     AND COMMAND = 'Query' AND ID <> CONNECTION_ID() AND \
                     (INFO LIKE 'CREATE%' OR INFO LIKE 'ALTER%' OR INFO LIKE 'DROP%' \
                      OR INFO LIKE 'RENAME%' OR INFO LIKE 'TRUNCATE%')",
                    (name,),
                )
                .await
            })
            .await?;
        Ok(n.unwrap_or(0) > 0)
    }
}
