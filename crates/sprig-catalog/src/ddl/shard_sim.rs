//! The multi-shard DDL state machine under crashes: a simulated tenant
//! whose tables live on several shards of a table group, changed through [`DdlEngine`] while the simulation
//! injects a crash at every step (before each shard, during each undo,
//! before publishing), refusals on one shard (a `CHECK` that a shard's rows
//! violate), and lost connections before or after a statement took effect.
//! After every run the reconciler's repair (itself crashed at random,
//! then run again) must leave:
//!
//! - no intent rows, no marks, no `creating` rows;
//! - every shard with the same definition of every object;
//! - the catalog's visible definition equal to the shards';
//! - either the old state everywhere or the new state everywhere.
//!
//! The simulated executor understands the subset of SQL the tests and the
//! undo path write: `CREATE TABLE`, `ALTER TABLE` with `ADD`, `DROP`,
//! `MODIFY` and table options, named algorithms (refused, as MariaDB does,
//! when the change needs a heavier one), `RENAME TABLE`, `DROP`, and
//! stored programs kept as text.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::if_same_then_else,
    clippy::nonminimal_bool,
    clippy::match_same_arms
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::json;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId, UserId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::exec::{DdlExecutor, Effect, PhysicalDdl, PhysicalObject, SchemaInfo, ShardTarget};
use super::shards::{Item, ItemKind, TableDef, comparable};
use super::*;
use crate::cache::mem::MemSource;
use crate::store::{ObjectKind, ObjectState};
use crate::version::DirectoryAllocator;

const T: TenantId = TenantId(4242);

fn shard(n: u32) -> SchemaName {
    SchemaName::shard(T, DbId(1), GroupId(2), ShardId(n))
}

fn template() -> SchemaName {
    SchemaName::shard(T, DbId(1), GroupId(1), ShardId(1))
}

/// An injected fault for one `execute` call (counted from 0 over the
/// whole run, undo statements included).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    LostBefore,
    LostAfter,
}

#[derive(Debug, Clone, Default)]
struct Schema {
    tables: BTreeMap<String, TableDef>,
    /// Other objects: their `SHOW CREATE` text.
    others: BTreeMap<(ObjectKind, String), String>,
    /// The rows here violate `qty > 0`.
    bad_rows: bool,
}

#[derive(Debug, Default)]
struct State {
    schemas: BTreeMap<SchemaName, Schema>,
    calls: usize,
    faults: BTreeMap<usize, Fault>,
    log: Vec<String>,
}

#[derive(Debug, Default)]
struct Sim {
    st: Mutex<State>,
    group: Vec<SchemaName>,
    /// Shards a simulated split holds (the mover's shard locks).
    held: Mutex<Vec<SchemaName>>,
}

fn backend(errno: u16, msg: &str) -> SprigError {
    SprigError::backend(errno, *b"HY000", msg.to_owned())
}

fn lost() -> SprigError {
    SprigError::new(SprigCode::OutcomeUnknown, "lost (simulated)")
}

/// The identifier after a keyword-free prefix: `` `a`.`b` `` → `b`.
fn last_ident(s: &str) -> String {
    let s = s.trim();
    let name = s.rsplit('.').next().unwrap_or(s);
    name.trim().trim_matches('`').to_owned()
}

/// Splits at top-level commas.
fn split_top(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for c in s.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => {
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '\'' | '`' | '"') => {
                quote = Some(c);
                cur.push(c);
            }
            (None, '(') => {
                depth += 1;
                cur.push(c);
            }
            (None, ')') => {
                depth -= 1;
                cur.push(c);
            }
            (None, ',') if depth == 0 => {
                out.push(cur.trim().to_owned());
                cur.clear();
            }
            (None, c) => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_owned());
    }
    out
}

fn up(s: &str) -> String {
    s.to_ascii_uppercase()
}

/// The heaviest algorithm a clause needs: 0 instant, 1 nocopy, 2
/// inplace, 3 copy.
fn level(clause: &str) -> u8 {
    let u = up(clause);
    if u.starts_with("ADD CONSTRAINT") && u.contains("CHECK") {
        3
    } else if u.starts_with("MODIFY") || u.contains("STORED") {
        3
    } else if u.starts_with("ADD KEY")
        || u.starts_with("ADD INDEX")
        || u.starts_with("ADD UNIQUE")
        || u.starts_with("DROP KEY")
        || u.starts_with("DROP INDEX")
    {
        1
    } else if u.starts_with("ADD PRIMARY") || u.starts_with("DROP PRIMARY") {
        2
    } else {
        0
    }
}

fn item_of(clause: &str) -> Option<Item> {
    let u = up(clause);
    let body = clause.trim();
    let after = |n: usize| body[n..].trim().to_owned();
    if u.starts_with("ADD COLUMN") {
        let t = after(10);
        return Some(Item {
            kind: ItemKind::Column(
                super::shards::TableDef::parse(&format!("CREATE TABLE `x` (\n  {t}\n) X")).map(
                    |d| match &d.items[0].kind {
                        ItemKind::Column(c) => c.clone(),
                        _ => String::new(),
                    },
                )?,
            ),
            text: t,
        });
    }
    let (rest, kind_text) = if u.starts_with("ADD UNIQUE KEY") || u.starts_with("ADD UNIQUE INDEX")
    {
        let n = if u.starts_with("ADD UNIQUE KEY") {
            14
        } else {
            16
        };
        (after(n), "UNIQUE KEY")
    } else if u.starts_with("ADD KEY") {
        (after(7), "KEY")
    } else if u.starts_with("ADD INDEX") {
        (after(9), "KEY")
    } else if u.starts_with("ADD PRIMARY KEY") {
        return Some(Item {
            kind: ItemKind::Key("PRIMARY".into()),
            text: format!("PRIMARY KEY {}", after(15)),
        });
    } else if u.starts_with("ADD CONSTRAINT") {
        let t = after(4);
        let d = TableDef::parse(&format!("CREATE TABLE `x` (\n  {t}\n) X"))?;
        return d.items.into_iter().next();
    } else if u.starts_with("ADD ") {
        let t = after(4);
        let d = TableDef::parse(&format!("CREATE TABLE `x` (\n  {t}\n) X"))?;
        return d.items.into_iter().next();
    } else {
        return None;
    };
    let name = rest.split_whitespace().next()?.trim_matches('`').to_owned();
    let cols = rest[rest.find('(')?..].to_owned();
    Some(Item {
        kind: ItemKind::Key(name.clone()),
        text: format!("{kind_text} `{name}` {cols}"),
    })
}

impl Sim {
    fn new(n: u32) -> Self {
        let s = Self {
            st: Mutex::new(State::default()),
            group: (1..=n).map(shard).collect(),
            held: Mutex::new(Vec::new()),
        };
        {
            let mut st = s.lock();
            for g in &s.group {
                st.schemas.insert(*g, Schema::default());
            }
        }
        s
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn snapshot(&self) -> BTreeMap<SchemaName, Schema> {
        self.lock().schemas.clone()
    }

    fn apply(st: &mut State, stmt: &PhysicalDdl) -> Result<()> {
        let sql = stmt.sql.trim();
        let u = up(sql);
        st.log.push(sql.to_owned());
        if matches!(stmt.effect, Effect::CreateSchema) {
            st.schemas.entry(stmt.schema).or_default();
            return Ok(());
        }
        if matches!(stmt.effect, Effect::DropSchema) {
            st.schemas.remove(&stmt.schema);
            return Ok(());
        }
        let sc = st
            .schemas
            .get_mut(&stmt.schema)
            .ok_or_else(|| backend(1049, "Unknown database"))?;
        match &stmt.effect {
            Effect::Create {
                kind: ObjectKind::Table,
                name,
            } if name.starts_with("__sprig_") => {
                // Internal tables (the decision table): not modelled.
            }
            Effect::Create {
                kind: ObjectKind::Table,
                name,
            } => {
                if sc.tables.contains_key(name) {
                    return Err(backend(1050, "Table already exists"));
                }
                let open = sql.find('(').unwrap();
                let close = sql.rfind(')').unwrap();
                let mut items = Vec::new();
                for c in split_top(&sql[open + 1..close]) {
                    let it = if up(&c).starts_with("PRIMARY KEY") {
                        item_of(&format!("ADD {c}"))
                    } else if c.starts_with('`') {
                        item_of(&format!("ADD COLUMN {c}"))
                    } else {
                        item_of(&format!("ADD {c}"))
                    };
                    items.push(it.ok_or_else(|| backend(1064, "syntax"))?);
                }
                sc.tables.insert(
                    name.clone(),
                    TableDef {
                        head: format!("CREATE TABLE `{name}` ("),
                        items,
                        options: "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4".to_owned(),
                    },
                );
            }
            Effect::Create { kind, name } | Effect::Restore { kind, name } => {
                let key = (*kind, name.to_ascii_lowercase());
                let replace = u.contains("OR REPLACE");
                if sc.others.contains_key(&key) && !replace {
                    return Err(backend(1304, "already exists"));
                }
                if *kind == ObjectKind::Trigger {
                    let t = u.rsplit(" ON ").next().unwrap_or("");
                    let t =
                        last_ident(t.split_whitespace().next().unwrap_or("")).to_ascii_lowercase();
                    if !sc.tables.keys().any(|k| k.to_ascii_lowercase() == t) {
                        return Err(backend(1146, "trigger table doesn't exist"));
                    }
                }
                let text = sql.replacen("CREATE OR REPLACE", "CREATE", 1);
                sc.others.insert(key, text);
            }
            Effect::Alter {
                kind: ObjectKind::Table,
                name,
            } => {
                let after_table = &sql[u.find("TABLE").unwrap() + 5..];
                let t = after_table.trim_start();
                let end = t.find(' ').unwrap_or(t.len());
                let tname = last_ident(&t[..end]);
                let rest = &t[end..];
                let mut def = sc
                    .tables
                    .get(&tname)
                    .cloned()
                    .ok_or_else(|| backend(1146, "Table doesn't exist"))?;
                let _ = name;
                let mut alg: Option<u8> = None;
                let mut clauses = Vec::new();
                for c in split_top(rest) {
                    let cu = up(&c);
                    if let Some(a) = cu.strip_prefix("ALGORITHM=") {
                        alg = Some(match a {
                            "INSTANT" => 0,
                            "NOCOPY" => 1,
                            "INPLACE" => 2,
                            _ => 3,
                        });
                    } else if cu.starts_with("LOCK=") {
                    } else {
                        clauses.push(c);
                    }
                }
                let need = clauses.iter().map(|c| level(c)).max().unwrap_or(0);
                if let Some(a) = alg
                    && a < need
                {
                    return Err(backend(1846, "ALGORITHM is not supported"));
                }
                for c in &clauses {
                    let cu = up(c);
                    if cu.starts_with("ADD") {
                        let it = item_of(c).ok_or_else(|| backend(1064, "syntax"))?;
                        if def.item(&it.kind).is_some() {
                            return Err(backend(1060, "Duplicate"));
                        }
                        if cu.contains("CHECK (`QTY` > 0)") && sc.bad_rows {
                            return Err(backend(4025, "CONSTRAINT `qty_pos` failed"));
                        }
                        // Columns before keys, as MariaDB prints them.
                        let pos = if matches!(it.kind, ItemKind::Column(_)) {
                            def.items
                                .iter()
                                .position(|i| !matches!(i.kind, ItemKind::Column(_)))
                                .unwrap_or(def.items.len())
                        } else {
                            def.items.len()
                        };
                        def.items.insert(pos, it);
                    } else if cu.starts_with("DROP") {
                        let words: Vec<&str> = c.split_whitespace().collect();
                        let kind = match up(words[1]).as_str() {
                            "COLUMN" => ItemKind::Column(last_ident(words[2])),
                            "PRIMARY" => ItemKind::Key("PRIMARY".into()),
                            "KEY" | "INDEX" => ItemKind::Key(last_ident(words[2])),
                            "CONSTRAINT" => ItemKind::Constraint {
                                name: last_ident(words[2]),
                                foreign: false,
                            },
                            "FOREIGN" => ItemKind::Constraint {
                                name: last_ident(words[3]),
                                foreign: true,
                            },
                            _ => ItemKind::Column(last_ident(words[1])),
                        };
                        let before = def.items.len();
                        let k2 = kind.clone();
                        def.items.retain(|i| {
                            !super::shards::TableDef {
                                head: String::new(),
                                items: vec![i.clone()],
                                options: String::new(),
                            }
                            .item(&k2)
                            .is_some()
                        });
                        if def.items.len() == before {
                            return Err(backend(1091, "Can't DROP; check that it exists"));
                        }
                    } else if cu.starts_with("MODIFY COLUMN") {
                        let text = c[13..].trim().to_owned();
                        let n = last_ident(text.split_whitespace().next().unwrap());
                        let i = def
                            .items
                            .iter_mut()
                            .find(|i| i.kind == ItemKind::Column(n.clone()))
                            .ok_or_else(|| backend(1054, "Unknown column"))?;
                        i.text = text;
                    } else if cu.starts_with("ENGINE=") {
                        def.options = c.replace(" COMMENT=''", "");
                    } else if cu.starts_with("COMMENT=") || cu == "COMMENT=''" {
                        let base: String = def
                            .options
                            .split(" COMMENT=")
                            .next()
                            .unwrap_or("")
                            .to_owned();
                        def.options = if c.trim() == "COMMENT=''" {
                            base
                        } else {
                            format!("{base} {c}")
                        };
                    } else {
                        return Err(backend(1064, "syntax (sim)"));
                    }
                }
                sc.tables.insert(tname, def);
            }
            Effect::Drop { kind, names } => {
                for n in names {
                    if *kind == ObjectKind::Table {
                        sc.tables.remove(n);
                        let n2 = n.to_ascii_lowercase();
                        sc.others.retain(|(k, _), text| {
                            *k != ObjectKind::Trigger
                                || !up(text).contains(&format!(" ON `{}`", up(&n2)))
                        });
                    } else {
                        sc.others.remove(&(*kind, n.to_ascii_lowercase()));
                    }
                }
            }
            Effect::Rename { moves } => {
                let mut next = sc.tables.clone();
                for (_, f, _, t) in moves {
                    let mut d = next
                        .remove(f)
                        .ok_or_else(|| backend(1146, "Table doesn't exist"))?;
                    if next.contains_key(t) {
                        return Err(backend(1050, "Table already exists"));
                    }
                    d.head = format!("CREATE TABLE `{t}` (");
                    next.insert(t.clone(), d);
                }
                sc.tables = next;
            }
            other => {
                return Err(SprigError::internal(format!("sim: unsupported {other:?}")));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl DdlExecutor for Sim {
    async fn execute(&self, stmt: &PhysicalDdl) -> Result<()> {
        let mut st = self.lock();
        let n = st.calls;
        st.calls += 1;
        match st.faults.remove(&n) {
            Some(Fault::LostBefore) => Err(lost()),
            Some(Fault::LostAfter) => {
                Self::apply(&mut st, stmt)?;
                Err(lost())
            }
            None => Self::apply(&mut st, stmt),
        }
    }

    async fn schema(&self, schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        Ok(self
            .lock()
            .schemas
            .contains_key(schema)
            .then(|| SchemaInfo {
                charset: "utf8mb4".into(),
                collation: "utf8mb4_uca1400_ai_ci".into(),
            }))
    }

    async fn show_create(
        &self,
        schema: &SchemaName,
        kind: ObjectKind,
        name: &str,
    ) -> Result<Option<String>> {
        let st = self.lock();
        let Some(sc) = st.schemas.get(schema) else {
            return Ok(None);
        };
        Ok(if kind == ObjectKind::Table {
            sc.tables.get(name).map(TableDef::render)
        } else {
            sc.others.get(&(kind, name.to_ascii_lowercase())).cloned()
        })
    }

    async fn objects(&self, schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        let st = self.lock();
        let mut out = Vec::new();
        if let Some(sc) = st.schemas.get(schema) {
            for t in sc.tables.keys() {
                out.push(PhysicalObject {
                    kind: ObjectKind::Table,
                    name: t.clone(),
                    parent: None,
                });
            }
            for (k, n) in sc.others.keys() {
                out.push(PhysicalObject {
                    kind: *k,
                    name: n.clone(),
                    parent: None,
                });
            }
        }
        out.sort();
        Ok(out)
    }

    async fn tenant_schemas(&self, tenant: TenantId) -> Result<Vec<SchemaName>> {
        Ok(self
            .lock()
            .schemas
            .keys()
            .filter(|s| s.tenant() == tenant)
            .copied()
            .collect())
    }

    async fn shards(
        &self,
        _tenant: TenantId,
        _db: &DatabaseRow,
        _target: ShardTarget<'_>,
    ) -> Result<Vec<SchemaName>> {
        Ok(self.group.clone())
    }

    fn cluster_of(&self, schema: &SchemaName) -> Option<ClusterId> {
        let i = self.group.iter().position(|s| s == schema)?;
        ClusterId::new(format!("sim-c{}", i % 2)).ok()
    }

    /// As the gateway's: poll the held shards, give up after 50 ms.
    async fn wait_for_shard_jobs(&self, _tenant: TenantId, shards: &[SchemaName]) -> Result<()> {
        for _ in 0..10 {
            let busy = self
                .held
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|h| shards.contains(h));
            if !busy {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err(SprigError::new(
            SprigCode::ShardBusy,
            "a split holds a shard (simulated)",
        ))
    }
}

// ---- the harness ----

async fn seed(store: &dyn MetaStore) {
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = json!({"v": 1, "tenant_id": T.0, "name": "sim", "state": "active",
                      "home_dc": "mum1", "catalog_version": 0, "auth_version": 0});
    store
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&body).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
}

struct Env {
    v: Versioner<MemSource>,
    sim: Arc<Sim>,
}

impl Env {
    async fn new(shards: u32) -> Self {
        let g = Arc::new(MemStore::new(Tier::Global));
        seed(&*g).await;
        let mem = MemSource::new();
        mem.bootstrap(T);
        let v = Versioner::new(mem, Arc::new(DirectoryAllocator::new(g)));
        let env = Self {
            v,
            sim: Arc::new(Sim::new(shards)),
        };
        env.engine(None)
            .execute(
                &req(
                    DdlStatement::CreateDatabase {
                        name: "shop".into(),
                        if_not_exists: false,
                        charset: None,
                        collation: None,
                    },
                    "CREATE DATABASE shop",
                ),
                &*env.sim,
            )
            .await
            .unwrap();
        env
    }

    fn engine(&self, crash: Option<DdlStep>) -> DdlEngine<MemSource> {
        let e = DdlEngine::new(self.v.clone(), Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(50),
            poll: Duration::from_millis(5),
            finish_read_attempts: 1,
            finish_read_backoff: Duration::from_millis(1),
        });
        match crash {
            None => e,
            Some(step) => e.with_failpoint(Arc::new(move |_, s| {
                if s == step {
                    Err(SprigError::internal(format!("crash at {s:?}")))
                } else {
                    Ok(())
                }
            })),
        }
    }

    async fn snap(&self) -> crate::store::Snapshot {
        self.v.backend().full_snapshot(T).await.unwrap()
    }

    /// Repairs every intent, as the reconciler would; `crash` makes the
    /// first rounds die at a step.
    async fn repair(&self, crash: Option<DdlStep>) {
        for round in 0..6 {
            let snap = self.snap().await;
            let intents = DdlIntent::all(&snap.settings);
            if intents.is_empty() {
                return;
            }
            let c = if round < 2 { crash } else { None };
            for i in intents {
                let _ = self.engine(c).resolve(T, &i, &snap, &*self.sim).await;
            }
        }
        let left = DdlIntent::all(&self.snap().await.settings);
        assert!(left.is_empty(), "intents left after repair: {left:?}");
    }
}

fn req(statement: DdlStatement, sql: &str) -> DdlRequest<'_> {
    DdlRequest {
        tenant: T,
        statement,
        sql,
        logical_sql: sql,
        current_db: Some("shop"),
        user: Some(UserId(1)),
        lower_case_table_names: false,
    }
}

fn tpl(sql: &str) -> String {
    sql.replace("{s}", &format!("`{}`", template()))
}

/// A statement of the simulation: what the engine is told, its text for
/// the template schema, and the object it touches.
struct Case {
    stmt: DdlStatement,
    sql: String,
}

fn alter_case(text: &str) -> Case {
    Case {
        stmt: DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::bare("orders"),
            if_exists: false,
            rename_to: None,
        },
        sql: tpl(&format!("ALTER TABLE {{s}}.`orders` {text}")),
    }
}

fn cases() -> Vec<Case> {
    vec![
        alter_case("ADD COLUMN `note` varchar(20) DEFAULT NULL"),
        alter_case("ADD KEY `by_qty` (`qty`)"),
        alter_case("ADD COLUMN `q2` int(11) GENERATED ALWAYS AS (`qty` * 2) VIRTUAL"),
        alter_case("ADD CONSTRAINT `qty_pos` CHECK (`qty` > 0)"),
        alter_case("MODIFY COLUMN `qty` bigint(20) DEFAULT NULL"),
        alter_case("DROP COLUMN `legacy`"),
        alter_case("ADD COLUMN `c2` int(11) DEFAULT NULL, ADD KEY `by_c2` (`c2`), COMMENT='v2'"),
        Case {
            stmt: DdlStatement::CreateObject {
                kind: ObjectKind::Procedure,
                name: ObjectName::bare("p"),
                if_not_exists: false,
                or_replace: false,
                parent: None,
            },
            sql: "CREATE DEFINER=`t4242_u1`@`%` PROCEDURE `p`() SELECT COUNT(*) FROM orders"
                .into(),
        },
        Case {
            stmt: DdlStatement::CreateObject {
                kind: ObjectKind::Trigger,
                name: ObjectName::bare("tr"),
                if_not_exists: false,
                or_replace: false,
                parent: Some(ObjectName::bare("orders")),
            },
            sql: "CREATE DEFINER=`t4242_u1`@`%` TRIGGER `tr` BEFORE INSERT ON `orders` FOR EACH ROW SET NEW.qty = NEW.qty"
                .into(),
        },
        Case {
            stmt: DdlStatement::CreateObject {
                kind: ObjectKind::Table,
                name: ObjectName::bare("extra"),
                if_not_exists: false,
                or_replace: false,
                parent: None,
            },
            sql: tpl("CREATE TABLE {s}.`extra` (`id` bigint(20) NOT NULL, PRIMARY KEY (`id`))"),
        },
        Case {
            stmt: DdlStatement::RenameTables {
                pairs: vec![(ObjectName::bare("orders"), ObjectName::bare("orders2"))],
                if_exists: false,
            },
            sql: tpl("RENAME TABLE {s}.`orders` TO {s}.`orders2`"),
        },
        Case {
            stmt: DdlStatement::DropObjects {
                kind: ObjectKind::Table,
                names: vec![ObjectName::bare("orders")],
                if_exists: false,
            },
            sql: tpl("DROP TABLE {s}.`orders`"),
        },
    ]
}

/// A tiny deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Every object's comparable text per shard: `None` where absent.
fn defs(
    state: &BTreeMap<SchemaName, Schema>,
    group: &[SchemaName],
) -> Vec<BTreeMap<String, String>> {
    group
        .iter()
        .map(|g| {
            let sc = state.get(g).cloned().unwrap_or_default();
            let mut m = BTreeMap::new();
            for (n, d) in &sc.tables {
                m.insert(format!("{} {n}", ObjectKind::Table), d.comparable());
            }
            for ((k, n), t) in &sc.others {
                m.insert(format!("{k} {n}"), t.clone());
            }
            m
        })
        .collect()
}

async fn check_converged(
    env: &Env,
    before: &[BTreeMap<String, String>],
    new: &BTreeMap<String, String>,
    label: &str,
) -> bool {
    let snap = env.snap().await;
    assert!(
        DdlIntent::all(&snap.settings).is_empty(),
        "{label}: intent left"
    );
    for o in &snap.objects {
        assert!(
            o.pending_job_id.is_none(),
            "{label}: mark left on {}",
            o.name
        );
        assert_ne!(
            o.state,
            ObjectState::Creating,
            "{label}: creating row {}",
            o.name
        );
    }
    let now = defs(&env.sim.snapshot(), &env.sim.group);
    for (i, d) in now.iter().enumerate() {
        assert_eq!(d, &now[0], "{label}: shard {i} differs from shard 0");
    }
    let applied = &now[0] == new;
    let old = now == before;
    assert!(
        applied || old,
        "{label}: neither old nor new state:\n{:#?}\nnew {:#?}",
        now[0],
        new
    );
    // The catalog's visible definitions match the shards.
    for o in snap
        .objects
        .iter()
        .filter(|o| o.state == ObjectState::Active)
    {
        let key = format!(
            "{} {}",
            o.kind,
            if o.kind == ObjectKind::Table {
                o.name.clone()
            } else {
                o.name.to_ascii_lowercase()
            }
        );
        let shard_def = now[0].get(&key);
        assert!(
            shard_def.is_some(),
            "{label}: catalog has {key} the shards don't"
        );
        let latest = env
            .v
            .backend()
            .tables(T)
            .unwrap()
            .schema_versions
            .values()
            .filter(|v| v.object_id == o.object_id)
            .max_by_key(|v| v.version)
            .and_then(|v| v.create_sql.clone());
        if let Some(l) = latest {
            assert_eq!(
                &comparable(&l),
                shard_def.unwrap(),
                "{label}: catalog differs for {key}"
            );
        }
    }
    for key in now[0].keys() {
        let (kind, name) = key.split_once(' ').unwrap();
        assert!(
            snap.objects.iter().any(|o| o.state == ObjectState::Active
                && o.kind.to_string() == kind
                && o.name.eq_ignore_ascii_case(name)),
            "{label}: shards have {key} the catalog doesn't"
        );
    }
    applied
}

/// The state the statement leads to on a fault-free copy.
fn expected(env: &Env, case: &Case) -> BTreeMap<String, String> {
    let mut st = State {
        schemas: env.sim.snapshot(),
        ..State::default()
    };
    let g = env.sim.group[0];
    let effect = match &case.stmt {
        DdlStatement::AlterObject { name, .. } => Effect::Alter {
            kind: ObjectKind::Table,
            name: name.name.clone(),
        },
        DdlStatement::CreateObject { kind, name, .. } => Effect::Create {
            kind: *kind,
            name: name.name.clone(),
        },
        DdlStatement::RenameTables { pairs, .. } => Effect::Rename {
            moves: pairs
                .iter()
                .map(|(a, b)| (g, a.name.clone(), g, b.name.clone()))
                .collect(),
        },
        DdlStatement::DropObjects { names, .. } => Effect::Drop {
            kind: ObjectKind::Table,
            names: names.iter().map(|n| n.name.clone()).collect(),
        },
        _ => unreachable!(),
    };
    let stmt = PhysicalDdl {
        schema: g,
        sql: super::shards::for_shard(&case.sql, &template(), &g),
        effect,
    };
    // A shard whose rows violate the CHECK makes the new state the old.
    let r = Sim::apply(&mut st, &stmt);
    let all = defs(&st.schemas, &[g]).remove(0);
    if r.is_err() {
        defs(&env.sim.snapshot(), &[g]).remove(0)
    } else {
        all
    }
}

async fn setup(env: &Env, bad_shard: Option<usize>) {
    let create = Case {
        stmt: DdlStatement::CreateObject {
            kind: ObjectKind::Table,
            name: ObjectName::bare("orders"),
            if_not_exists: false,
            or_replace: false,
            parent: None,
        },
        sql: tpl(
            "CREATE TABLE {s}.`orders` (`id` bigint(20) NOT NULL, `qty` int(11) DEFAULT NULL, `legacy` int(11) DEFAULT NULL, PRIMARY KEY (`id`))",
        ),
    };
    env.engine(None)
        .execute(&req(create.stmt, &create.sql), &*env.sim)
        .await
        .unwrap();
    if let Some(b) = bad_shard {
        let g = env.sim.group[b];
        env.sim.lock().schemas.get_mut(&g).unwrap().bad_rows = true;
    }
}

#[tokio::test]
async fn every_statement_reaches_every_shard() {
    for case in cases() {
        let env = Env::new(4).await;
        setup(&env, None).await;
        let before = defs(&env.sim.snapshot(), &env.sim.group);
        let new = expected(&env, &case);
        let out = env
            .engine(None)
            .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
            .await;
        assert!(out.is_ok(), "{}: {out:?}", case.sql);
        assert!(
            check_converged(&env, &before, &new, &case.sql).await,
            "{} not applied",
            case.sql
        );
    }
}

#[tokio::test]
async fn a_check_violated_on_one_shard_rolls_back_everywhere() {
    let env = Env::new(4).await;
    setup(&env, Some(2)).await;
    let before = defs(&env.sim.snapshot(), &env.sim.group);
    let case = alter_case("ADD CONSTRAINT `qty_pos` CHECK (`qty` > 0)");
    let e = env
        .engine(None)
        .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
        .await
        .unwrap_err();
    assert_eq!(errno(&e), Some(4025));
    assert!(!check_converged(&env, &before, &BTreeMap::new(), "check").await);
    // The undo of shards 0 and 1 dropped the constraint again.
    let log = env.sim.lock().log.clone();
    assert!(
        log.iter().any(|l| l.contains("DROP CONSTRAINT `qty_pos`")),
        "{log:#?}"
    );
}

#[tokio::test]
async fn drifted_shards_refuse_a_change() {
    let env = Env::new(3).await;
    setup(&env, None).await;
    // An outside change on shard 2 only.
    let g = env.sim.group[1];
    Sim::apply(
        &mut env.sim.lock(),
        &PhysicalDdl {
            schema: g,
            sql: format!("ALTER TABLE `{g}`.`orders` ADD COLUMN `x` int(11) DEFAULT NULL"),
            effect: Effect::Alter {
                kind: ObjectKind::Table,
                name: "orders".into(),
            },
        },
    )
    .unwrap();
    let case = alter_case("ADD COLUMN `note` varchar(20) DEFAULT NULL");
    let e = env
        .engine(None)
        .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::DdlShardDrift));
    assert!(DdlIntent::all(&env.snap().await.settings).is_empty());
}

/// The crash simulation: every case × crash points × faults, seeded.
#[tokio::test]
async fn crashes_at_every_step_converge() {
    let runs: u64 = std::env::var("SPRIG_DDL_SIM_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let n_cases = cases().len() as u64;
    let (mut applied, mut rolled_back) = (0u32, 0u32);
    for seed in 1..=runs {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let shards = 2 + rng.below(3) as u32;
        let env = Env::new(shards).await;
        let bad = (rng.below(3) == 0).then(|| rng.below(u64::from(shards)) as usize);
        setup(&env, bad).await;
        let case = &cases()[rng.below(n_cases) as usize];
        let before = defs(&env.sim.snapshot(), &env.sim.group);
        let new = expected(&env, case);
        let steps: Vec<DdlStep> = {
            let mut v = vec![DdlStep::Planned, DdlStep::Prepared, DdlStep::Applied];
            for i in 0..shards as usize {
                v.push(DdlStep::Shard(i));
                v.push(DdlStep::Undo(i));
            }
            v
        };
        let crash = match rng.below(3) {
            0 => None,
            _ => Some(steps[rng.below(steps.len() as u64) as usize]),
        };
        // A lost connection at some execute call.
        if rng.below(2) == 0 {
            let call = env.sim.lock().calls + rng.below(u64::from(shards) * 3) as usize;
            let f = if rng.below(2) == 0 {
                Fault::LostBefore
            } else {
                Fault::LostAfter
            };
            env.sim.lock().faults.insert(call, f);
        }
        let label = format!(
            "seed {seed}: {} shards, bad {bad:?}, crash {crash:?}, {}",
            shards, case.sql
        );
        let out = env
            .engine(crash)
            .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
            .await;
        env.sim.lock().faults.clear();
        let repair_crash =
            (rng.below(2) == 0).then(|| steps[rng.below(steps.len() as u64) as usize]);
        env.repair(repair_crash).await;
        let done = check_converged(&env, &before, &new, &label).await;
        if out.is_ok() {
            assert!(done || before[0] == new, "{label}: OK but not applied");
        }
        if done {
            applied += 1;
        } else {
            rolled_back += 1;
        }
    }
    eprintln!(
        "crash simulation: {runs} runs, {applied} all applied, {rolled_back} all rolled back"
    );
}

#[tokio::test]
async fn a_running_job_holds_its_shards_for_splits() {
    let env = Env::new(3).await;
    setup(&env, None).await;
    let case = alter_case("ADD COLUMN `note` varchar(20) DEFAULT NULL");
    // Crash after the first shard: the job's intent row stays.
    let _ = env
        .engine(Some(DdlStep::Shard(1)))
        .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
        .await
        .unwrap_err();
    let rows = env.snap().await.settings;
    assert_eq!(jobs_on_shards(&rows, &[shard(2)]).len(), 1);
    assert!(jobs_on_shards(&rows, &[shard(9)]).is_empty());
    let i = &DdlIntent::all(&rows)[0];
    let set = i.shards.as_ref().unwrap();
    assert_eq!(set.shards.len(), 3);
    assert_eq!(set.shards[1].cluster.as_deref(), Some("sim-c1"));
    env.repair(None).await;
    assert!(jobs_on_shards(&env.snap().await.settings, &[shard(2)]).is_empty());
}

/// A schema change waits for a split that holds one of its shards, and
/// refuses when the wait runs out, changing nothing.
#[tokio::test]
async fn a_change_refuses_while_a_split_holds_a_shard() {
    let env = Env::new(3).await;
    setup(&env, None).await;
    let before = defs(&env.sim.snapshot(), &env.sim.group);
    env.sim.held.lock().unwrap().push(shard(2));
    let case = alter_case("ADD COLUMN `note` varchar(20) DEFAULT NULL");
    let e = env
        .engine(None)
        .execute(&req(case.stmt.clone(), &case.sql), &*env.sim)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardBusy));
    assert!(e.retryable);
    // No shard changed, no intent row left, the table not pending.
    assert_eq!(defs(&env.sim.snapshot(), &env.sim.group), before);
    assert!(DdlIntent::all(&env.snap().await.settings).is_empty());
    assert!(env.sim.lock().log.iter().all(|l| !l.contains("note")));
}

#[tokio::test]
async fn a_change_waits_for_a_split_and_holds_its_shards_meanwhile() {
    let env = Env::new(3).await;
    setup(&env, None).await;
    env.sim.held.lock().unwrap().push(shard(2));
    let case = alter_case("ADD COLUMN `note` varchar(20) DEFAULT NULL");
    let new = expected(&env, &case);
    let before = defs(&env.sim.snapshot(), &env.sim.group);
    let run = {
        let engine = env.engine(None);
        let sim = Arc::clone(&env.sim);
        let stmt = case.stmt.clone();
        let sql = case.sql.clone();
        tokio::spawn(async move { engine.execute(&req(stmt, &sql), &*sim).await.map(|_| ()) })
    };
    // While it waits, its intent row holds the shards: a new split of
    // any of them would wait for it (the mover's side).
    let mut seen = false;
    for _ in 0..20 {
        let rows = env.snap().await.settings;
        if !jobs_on_shards(&rows, &[shard(3)]).is_empty() {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(seen, "the waiting job's shards were not held");
    // The split ends; the change goes ahead on every shard.
    env.sim.held.lock().unwrap().clear();
    run.await.unwrap().unwrap();
    assert!(check_converged(&env, &before, &new, &case.sql).await);
}
