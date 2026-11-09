//! An in-memory [`DdlExecutor`]: schemas and the objects in them, changed
//! by each statement's [`Effect`] (the SQL is only logged), with injected
//! faults for crash tests: a definite server error, or a lost connection
//! before or after the statement took effect.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};

use sprig_common::ids::TenantId;
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};

use super::exec::{DdlExecutor, Effect, PhysicalDdl, PhysicalObject, SchemaInfo, kind_keyword};
use crate::store::{Namespace, ObjectKind};

/// A fault for the next [`DdlExecutor::execute`].
#[derive(Debug)]
pub enum Fault {
    /// The server refuses the statement with this error; nothing changes.
    Refuse(SprigError),
    /// The connection is lost before the statement reaches the server.
    LostBefore,
    /// The statement takes effect, then the connection is lost before the
    /// reply.
    LostAfter,
    /// The statement runs normally (to aim a later fault at a plan's
    /// second statement).
    Pass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemObject {
    kind: ObjectKind,
    name: String,
    parent: Option<String>,
    rev: u64,
}

#[derive(Debug, Clone, Default)]
struct MemSchema {
    charset: String,
    collation: String,
    /// By (namespace, comparison key).
    objects: BTreeMap<(Namespace, String), MemObject>,
}

#[derive(Debug, Default)]
struct State {
    schemas: BTreeMap<SchemaName, MemSchema>,
    faults: VecDeque<Fault>,
    log: Vec<PhysicalDdl>,
    busy: Vec<SchemaName>,
    /// Reads (`schema`, `show_create`) still to fail with a lost
    /// connection.
    failing_reads: u32,
}

/// The in-memory executor (see the module docs).
#[derive(Debug, Default)]
pub struct MemExecutor {
    state: Mutex<State>,
}

fn key(kind: ObjectKind, name: &str) -> (Namespace, String) {
    let ns = kind.namespace();
    let k = match ns {
        Namespace::Procedure | Namespace::Function | Namespace::Event => name.to_lowercase(),
        _ => name.to_owned(),
    };
    (ns, k)
}

fn err(errno: u16, state: [u8; 5], msg: String) -> SprigError {
    SprigError::backend(errno, state, msg)
}

fn has(sql: &str, words: &str) -> bool {
    sql.to_ascii_uppercase().contains(words)
}

impl MemExecutor {
    /// No schemas.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues a fault for the next statement.
    pub fn inject(&self, f: Fault) {
        self.lock().faults.push_back(f);
    }

    /// Makes the next `n` reads (`schema`, `show_create`) fail with a lost
    /// connection, as phase 3's read would during a network blip.
    pub fn fail_reads(&self, n: u32) {
        self.lock().failing_reads = n;
    }

    /// A read fails while `fail_reads` has some left.
    fn read_fault(&self) -> Result<()> {
        let mut s = self.lock();
        if s.failing_reads > 0 {
            s.failing_reads -= 1;
            return Err(lost());
        }
        Ok(())
    }

    /// Marks a schema as running DDL (or not), for [`DdlExecutor::busy`].
    pub fn set_busy(&self, schema: SchemaName, busy: bool) {
        let mut s = self.lock();
        s.busy.retain(|b| *b != schema);
        if busy {
            s.busy.push(schema);
        }
    }

    /// Every statement executed so far (faulted ones included).
    #[must_use]
    pub fn log(&self) -> Vec<PhysicalDdl> {
        self.lock().log.clone()
    }

    /// Whether `schema` exists.
    #[must_use]
    pub fn has_schema(&self, schema: &SchemaName) -> bool {
        self.lock().schemas.contains_key(schema)
    }

    /// Whether `schema` holds the object `name` of `kind`.
    #[must_use]
    pub fn has_object(&self, schema: &SchemaName, kind: ObjectKind, name: &str) -> bool {
        self.lock()
            .schemas
            .get(schema)
            .is_some_and(|s| s.objects.values().any(|o| o.kind == kind && o.name == name))
    }

    /// The names of `kind` objects in `schema`, sorted; internal
    /// (`__sprig_`) objects, such as the decision table, left out.
    #[must_use]
    pub fn names(&self, schema: &SchemaName, kind: ObjectKind) -> Vec<String> {
        self.lock()
            .schemas
            .get(schema)
            .map(|s| {
                s.objects
                    .values()
                    .filter(|o| o.kind == kind && !o.name.starts_with("__sprig_"))
                    .map(|o| o.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Creates a schema directly (an outside change).
    pub fn create_schema(&self, schema: SchemaName) {
        self.lock()
            .schemas
            .entry(schema)
            .or_insert_with(|| MemSchema {
                charset: "utf8mb4".to_owned(),
                collation: "utf8mb4_uca1400_ai_ci".to_owned(),
                objects: BTreeMap::new(),
            });
    }

    /// Creates an object directly (an outside change). `false` if the
    /// schema is missing.
    pub fn create_object(&self, schema: &SchemaName, obj: PhysicalObject) -> bool {
        let mut s = self.lock();
        let Some(sc) = s.schemas.get_mut(schema) else {
            return false;
        };
        sc.objects.insert(
            key(obj.kind, &obj.name),
            MemObject {
                kind: obj.kind,
                name: obj.name,
                parent: obj.parent,
                rev: 1,
            },
        );
        true
    }

    /// Drops an object directly (an outside change).
    pub fn drop_object(&self, schema: &SchemaName, kind: ObjectKind, name: &str) {
        if let Some(sc) = self.lock().schemas.get_mut(schema) {
            sc.objects.remove(&key(kind, name));
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per effect
    fn apply(s: &mut State, stmt: &PhysicalDdl) -> Result<()> {
        let schema = stmt.schema;
        let sql = stmt.sql.as_str();
        match &stmt.effect {
            Effect::CreateSchema => match s.schemas.entry(schema) {
                std::collections::btree_map::Entry::Occupied(_) => {
                    if !has(sql, "IF NOT EXISTS") {
                        return Err(err(
                            1007,
                            *b"HY000",
                            format!("Can't create database '{schema}'; database exists"),
                        ));
                    }
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    let charset = if has(sql, "LATIN1") {
                        "latin1"
                    } else {
                        "utf8mb4"
                    };
                    v.insert(MemSchema {
                        charset: charset.to_owned(),
                        collation: format!("{charset}_general_ci"),
                        objects: BTreeMap::new(),
                    });
                }
            },
            Effect::AlterSchema => {
                let sc = s
                    .schemas
                    .get_mut(&schema)
                    .ok_or_else(|| err(1049, *b"42000", format!("Unknown database '{schema}'")))?;
                if has(sql, "LATIN1") {
                    "latin1".clone_into(&mut sc.charset);
                    "latin1_general_ci".clone_into(&mut sc.collation);
                }
            }
            Effect::DropSchema => {
                if s.schemas.remove(&schema).is_none() && !has(sql, "IF EXISTS") {
                    return Err(err(
                        1008,
                        *b"HY000",
                        format!("Can't drop database '{schema}'; database doesn't exist"),
                    ));
                }
            }
            Effect::Create { kind, name } => {
                let parent = if *kind == ObjectKind::Trigger {
                    // `CREATE TRIGGER t ... ON tbl`: the test SQL ends with
                    // `ON <table>`.
                    sql.rsplit_once(" ON ")
                        .map(|(_, t)| t.trim().trim_matches('`').to_owned())
                } else {
                    None
                };
                let sc = s
                    .schemas
                    .get_mut(&schema)
                    .ok_or_else(|| err(1049, *b"42000", format!("Unknown database '{schema}'")))?;
                let k = key(*kind, name);
                if let Some(o) = sc.objects.get_mut(&k) {
                    if has(sql, "OR REPLACE") {
                        o.rev += 1;
                        return Ok(());
                    }
                    if has(sql, "IF NOT EXISTS") {
                        return Ok(());
                    }
                    return Err(err(
                        1050,
                        *b"42S01",
                        format!("{} '{name}' already exists", kind_keyword(*kind)),
                    ));
                }
                if let Some(p) = &parent
                    && !sc.objects.contains_key(&key(ObjectKind::Table, p))
                {
                    return Err(err(
                        1146,
                        *b"42S02",
                        format!("Table '{schema}.{p}' doesn't exist"),
                    ));
                }
                sc.objects.insert(
                    k,
                    MemObject {
                        kind: *kind,
                        name: name.clone(),
                        parent,
                        rev: 1,
                    },
                );
            }
            Effect::Alter { kind, name } | Effect::Restore { kind, name } => {
                let o = s
                    .schemas
                    .get_mut(&schema)
                    .and_then(|sc| sc.objects.get_mut(&key(*kind, name)))
                    .ok_or_else(|| {
                        err(
                            1146,
                            *b"42S02",
                            format!("Table '{schema}.{name}' doesn't exist"),
                        )
                    })?;
                if has(sql, "BROKEN") {
                    return Err(err(
                        1064,
                        *b"42000",
                        "You have an error in your SQL syntax".to_owned(),
                    ));
                }
                o.rev += 1;
            }
            Effect::Drop { kind, names } => {
                let sc = s
                    .schemas
                    .get_mut(&schema)
                    .ok_or_else(|| err(1049, *b"42000", format!("Unknown database '{schema}'")))?;
                let missing: Vec<&String> = names
                    .iter()
                    .filter(|n| !sc.objects.contains_key(&key(*kind, n)))
                    .collect();
                if !missing.is_empty() && !has(sql, "IF EXISTS") {
                    return Err(err(
                        1051,
                        *b"42S02",
                        format!("Unknown table '{schema}.{}'", missing[0]),
                    ));
                }
                for n in names {
                    sc.objects.remove(&key(*kind, n));
                    if matches!(kind, ObjectKind::Table) {
                        sc.objects.retain(|_, o| o.parent.as_deref() != Some(n));
                    }
                }
            }
            Effect::Rename { moves } => {
                // Check every move first: RENAME TABLE is atomic.
                let mut next = s.schemas.clone();
                for (fs, f, ts, t) in moves {
                    let obj = next
                        .get_mut(fs)
                        .and_then(|sc| sc.objects.remove(&key(ObjectKind::Table, f)))
                        .ok_or_else(|| {
                            err(1146, *b"42S02", format!("Table '{fs}.{f}' doesn't exist"))
                        })?;
                    let target = next
                        .get_mut(ts)
                        .ok_or_else(|| err(1049, *b"42000", format!("Unknown database '{ts}'")))?;
                    let k = key(obj.kind, t);
                    if target.objects.contains_key(&k) {
                        return Err(err(1050, *b"42S01", format!("Table '{t}' already exists")));
                    }
                    let mut obj = obj;
                    obj.name.clone_from(t);
                    obj.rev += 1;
                    target.objects.insert(k, obj);
                    for o in target.objects.values_mut() {
                        if o.parent.as_deref() == Some(f.as_str()) {
                            o.parent = Some(t.clone());
                        }
                    }
                }
                s.schemas = next;
            }
            Effect::Truncate { name } => {
                if !s
                    .schemas
                    .get(&schema)
                    .is_some_and(|sc| sc.objects.contains_key(&key(ObjectKind::Table, name)))
                {
                    return Err(err(
                        1146,
                        *b"42S02",
                        format!("Table '{schema}.{name}' doesn't exist"),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn lost() -> SprigError {
    SprigError::new(
        SprigCode::OutcomeUnknown,
        "the backend was lost during DDL (test)",
    )
}

#[async_trait::async_trait]
impl DdlExecutor for MemExecutor {
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        let mut s = self.lock();
        s.log.push(stmt.clone());
        match s.faults.pop_front() {
            Some(Fault::Refuse(e)) => Err(e),
            Some(Fault::LostBefore) => Err(lost()),
            Some(Fault::LostAfter) => {
                Self::apply(&mut s, stmt)?;
                Err(lost())
            }
            Some(Fault::Pass) | None => Self::apply(&mut s, stmt),
        }
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        self.read_fault()?;
        Ok(self.lock().schemas.get(schema).map(|s| SchemaInfo {
            charset: s.charset.clone(),
            collation: s.collation.clone(),
        }))
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        self.read_fault()?;
        Ok(self
            .lock()
            .schemas
            .get(schema)
            .and_then(|s| s.objects.get(&key(kind, name)))
            .map(|o| {
                format!(
                    "CREATE {} `{}` /* rev {} */",
                    kind_keyword(o.kind),
                    o.name,
                    o.rev
                )
            }))
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        let mut out: Vec<PhysicalObject> = self
            .lock()
            .schemas
            .get(schema)
            .map(|s| {
                s.objects
                    .values()
                    .map(|o| PhysicalObject {
                        kind: o.kind,
                        name: o.name.clone(),
                        parent: o.parent.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        Ok(out)
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        Ok(self
            .lock()
            .schemas
            .keys()
            .filter(|s| s.tenant() == tenant && s.db().is_some())
            .copied()
            .collect())
    }

    async fn busy(&self, schema: &SchemaName) -> Result<bool> {
        Ok(self.lock().busy.contains(schema))
    }
}
